# LDSCI7229 Advanced Data Engineering - AE1 Report

**GitHub:** https://github.com/catliugit/advanced-data-engineering-ae1

---

# Task 1 - Serverless Ingestion Pipeline


## 1. Dataset explanation

For Task 1 I designed a serverless ingestion pipeline around two deliberately contrasting datasets so I could exercise both batch and streaming patterns inside one architecture.

The first is the **US DOT Border Crossing Entry Data**, published by the US Bureau of Transportation Statistics. It contains roughly 273,000 rows dating back to 1996, where each row represents the monthly count of vehicles, pedestrians, or passengers passing through a US land port. Columns include port name, state, border (US-Canada or US-Mexico), date, transport measure, value, and a geographic point. It is a static historical dataset that updates infrequently, making it a natural fit for batch ingestion.

The second is **OpenAlex**, a free open academic graph exposing a REST API for research papers, authors, institutions, and citations. Each "work" is returned as a nested JSON object containing arrays of authors, institutions, and per-year citation counts. Because OpenAlex is continuously updated and best consumed via API calls rather than bulk dumps, I treated it as a streaming source.

Basically these two sources are different in every way: CSV vs JSON, flat vs nested, one-shot download vs continuous API calls. That's why I picked them, because if my pipeline can handle both, it can probably handle most things.

## 2. Pipeline design principles

The pipeline is fully serverless, a choice driven both by the AWS Academy Learner Lab constraints (no custom IAM, no VPCs, no Bedrock/Redshift/EMR) and by the broader goal of building something that scales without provisioning compute by hand.

I structured the S3 data lake into three zones: `raw/` for untouched source data, `processed/` reserved for staging, and `cleaned/` for validated Parquet output (**Figure 1**). This separation guarantees that raw data is never overwritten, so I can reprocess at any time without re-ingesting from the source.

For streaming, a Lambda function (`openalex-ingestion`) calls the OpenAlex API in pages and forwards each page as a JSON record to a Kinesis Data Firehose stream (**Figure 2**). Firehose buffers up to five minutes or 5 MB of records before writing a batch file to `raw/openalex/`. The Lambda is triggered hourly by an EventBridge schedule, so the lake fills incrementally without me touching anything.

For batch, the border crossing CSV is uploaded directly to `raw/`. Two AWS Glue jobs (`border-crossing-etl` and `openalex-etl`) then read each dataset, clean it, and write Parquet output to `cleaned/`.

The entire flow is wrapped in a single Step Functions state machine called `data-pipeline` (**Figure 3**). It first invokes the Lambda, then runs both Glue jobs in parallel using a `Parallel` state, and finally records success in DynamoDB. I chose **Standard** Step Functions rather than Express, because Express workflows have a five-minute hard execution limit and my Glue jobs routinely run longer than that - Standard also gives me a full execution history I can inspect retroactively, which proved useful when debugging failed runs. Every step has `Retry` for transient errors and `Catch` blocks that route to a `PipelineFailed` terminal state, so I get error handling for free without writing extra code. The result is a pipeline I can launch end-to-end with one click and that gracefully recovers from most transient failures.

## 3. Streaming vs batch ingestion trade-offs

Batch and streaming are very different beasts. The main question is how fresh the data needs to be versus how much hassle you're willing to deal with.

**Batch ingestion** (border crossing CSV) is simple and predictable. There is no buffering, no event-driven plumbing, and no race conditions: a file lands in S3 and Glue picks it up. The trade-off is freshness, since the data is only as current as the last upload, which makes batch unsuitable for anything requiring real-time insight. Reprocessing is trivial because the input is a single file; rerunning the Glue job with `mode("overwrite")` won't create duplicates if you run it twice. Cost is low because S3 storage is cheap and the Glue job only runs on demand.

**Streaming ingestion** (OpenAlex via Lambda and Firehose) gives much fresher data but introduces several new failure modes. Lambda timeouts, Firehose throttling, EventBridge scheduling drift, and OpenAlex rate limits all become things I need to consider. Debugging is harder because instead of one well-defined input file there are hundreds of small JSON objects arriving asynchronously. I deliberately used Firehose rather than having Lambda write directly to S3 because Firehose batches records into larger files every five minutes, which cuts down on the small-file problem that would slow Glue down later.

The border crossing dataset is static and analytical, so the simplicity of batch wins. OpenAlex is continuously updated and the use case (citation trends, recent papers) benefits from freshness, so the extra complexity of streaming is justified. Having both in one pipeline added complexity, but it forced me to make the downstream cleaning and metadata logging generic enough to work for either pattern.

## 4. Data transformation and quality control considerations

Both Glue ETL jobs use PySpark and follow the same shape: extract from `raw/`, transform the schema, validate row by row, and write to `cleaned/` as Parquet.

The `border-crossing-etl` script reads the CSV with header inference, drops a redundant column, parses the `Date` field into a proper timestamp, casts numeric counts to integers, and drops any rows with null values in key columns (port, value, date). Malformed rows (the source CSV does contain a few) are silently discarded rather than allowed to poison downstream queries.

The `openalex-etl` script is more involved because the source is deeply nested JSON. It reads every JSON file under `raw/openalex/`, then uses Spark's `select`, `explode`, and `getField` operations to flatten the structure into a flat schema (title, publication date, citation count, primary author, primary institution, country). Missing fields coalesce to nulls rather than failing the job.

Both scripts write to `cleaned/` as **Parquet with Snappy compression**. Parquet is columnar, so the Athena queries in Task 2 only scan the columns they need, which makes the warehouse layer cheap and fast. Using `mode("overwrite")` keeps every job idempotent, so reruns produce the same result rather than appending duplicates.

Quality control extends beyond the scripts themselves. Each Glue job writes a record to a DynamoDB table called `pipeline-metadata` after each run, capturing job name, run timestamp, source path, rows processed, rows cleaned, duration, and status (**Figure 4**). This gives me a log I can always go back to. I can answer "did this job run, how many rows, how long did it take" without digging through CloudWatch.

Finally, the Step Functions `Catch` blocks catch anything that goes wrong. If a step throws an error the whole pipeline stops and you can see exactly where it failed in the graph (**Figure 5**), so failures are caught at the orchestration layer rather than silently producing bad data.

---

## Figures

![Figure 1: S3 data lake showing raw/, processed/, cleaned/ zones](screenshots/task1-p1-s3-bucket-structure.png)

![Figure 2: Kinesis Data Firehose delivery stream configured to write to raw/openalex/](screenshots/task1-p2-firehose-config.png)

![Figure 3: data-pipeline Step Functions state machine, Type: Standard, Status: Active](screenshots/task1-p5-stepfunctions-overview.png)

![Figure 4: DynamoDB pipeline-metadata table with example log entries from successful runs](screenshots/task1-p4-dynamodb-entries.png)

![Figure 5: Successful Step Functions execution graph with all states green](screenshots/task1-p5-stepfunctions-success.png)

---

# Task 2 - Data Warehouse Development


## 1. Architecture overview

The data warehouse for Task 2 sits on top of the cleaned Parquet files produced in Task 1. The flow is short: the cleaned data lives in `s3://ae1-data-lake-catliugit/cleaned/`, two Glue Crawlers scan those folders and register the schemas in the Glue Data Catalog under a database called `data_warehouse`, and Athena then queries those tables with standard SQL. There is no separate database server, no data load step, and no compute provisioning - Athena reads the Parquet files directly from S3 each time a query is run.

This is basically a **schema-on-read** setup. The schema isn't baked into the files; it lives in the Catalog and gets applied when you actually query the data. If I want to change column types or add new columns, I just rerun the crawler, no data migration needed.

## 2. Cataloguing with Glue Crawlers

I created two crawlers (`border-crossing-crawler` and `openalex-crawler`), each pointing at the corresponding subfolder under `cleaned/` (**Figure 6, Figure 7**). Both run on demand using the `LabRole` IAM role and write into the `data_warehouse` database.

After running them, both `border_crossing` and `openalex` tables appeared in the Catalog with classification `parquet` (**Figure 8**). The crawler inferred all column types automatically from the Parquet metadata - `string` for text columns like `port name` and `state`, `int` for `value`, `date` for `date`, and `double` for `latitude` / `longitude` (**Figure 9**). One quirk worth noting: because the source CSV had headers like "Port Name" with a space, the crawler kept the spaces in the column names, which means I have to quote them in SQL (`"port name"` instead of `port_name`). I left it that way to keep the catalog faithful to the source.

The `openalex` table was inferred similarly, with eight columns covering `work_id`, `title`, `publication_date`, `cited_by_count`, open access flags, and the comma-joined author/institution strings produced by the Glue ETL job (**Figure 10**).

## 3. Performance optimisation

Two design choices from Task 1 pay off heavily in Task 2:

**Parquet columnar format.** Athena only reads the columns referenced in a `SELECT` clause. When I queried `SELECT YEAR(date), SUM(value) FROM border_crossing`, Athena scanned just **1.01 MB** of data despite the table having over 273,000 rows across 9 columns (**Figure 11**). A `SELECT *` over the same table would scan many times more, because it would have to pull every column. This is the single most important reason to use Parquet over CSV in a lakehouse.

**Snappy compression.** Each Parquet file is Snappy-compressed on disk, so the bytes Athena has to read are smaller again. Snappy is not the best at compression ratio but it's fast to decompress, which matters more when Athena is scanning files on every query. Good enough compression with minimal overhead.

**Partitioning** would be the next step for a larger dataset. With 273k rows the warehouse is already small enough that partitioning would not change query costs much, but if I were ingesting millions of rows monthly I would partition `border_crossing` by year and `openalex` by `publication_date` year. Both fields are good partition candidates because most of my analytical queries filter or group by them.

## 4. Athena queries and analysis

I wrote five queries covering the analytical patterns the assignment asks for: trends over time, ranking, multi-dimensional aggregation, cross-format reading, and schema introspection. All five are saved in `task2-warehouse/athena-queries/`.

**Query 1 - Trend over time** (`01_yearly_border_crossings.sql`): aggregates total crossings by year. The result shows a steady drop from a peak around 1999–2000 (~527 million crossings) down through the early 2000s (**Figure 12**). Run time was ~523 ms, scanning ~1 MB.

**Query 2 - Top 10 busiest ports** (`02_top_busiest_ports.sql`): groups by port and state, sums the volume, ranks the result. San Ysidro (California) tops the list with ~1.45 billion total crossings, followed by El Paso, Laredo, and Hidalgo in Texas (**Figure 13**). This is where Parquet really helps, since Athena only had to read three columns out of nine, so the scan was tiny.

**Query 3 - Top 20 cited OpenAlex papers** (`03_top_cited_openalex_papers.sql`): orders the papers by `cited_by_count` (**Figure 14**). I noticed early on that the results contained duplicates - the same paper appearing several times. The cause is in the Lambda: it always starts pagination from cursor `*` so each scheduled run re-fetches the same first batch of recent works. Rather than rebuild the Lambda I added `DISTINCT` to the query, which is a reasonable schema-on-read fix and shows the flexibility of querying raw lake data - I can patch logic at the query layer without touching storage.

**Query 4 - Multi-dimensional aggregation** (`04_yearly_crossings_by_border.sql`): splits totals by year *and* border (US-Canada vs US-Mexico), and additionally counts distinct ports and average crossings per record (**Figure 15**). This was the most interesting result: the US-Mexico border consistently sees roughly twice the volume of the US-Canada border across the time series.

**Query 5 - Schema-on-read demonstration** (`05_schema_on_read_demo.sql`): runs `DESCRIBE border_crossing` to print the schema (**Figure 16**) and `SHOW CREATE TABLE` to print the full external table definition including the S3 location, SerDe, and TBLPROPERTIES (**Figure 17**). The `SHOW CREATE TABLE` output is good evidence of schema-on-read: the table is declared as `EXTERNAL` and points at `s3://ae1-data-lake-catliugit/cleaned/border_crossing/` - Athena does not own the data, it just reads it through the catalog definition.

## 5. Monitoring with CloudWatch

CloudWatch collects logs and metrics from every service in the pipeline without any manual setup. I used it to verify job execution history and query performance, which is what the spec asks for.

On the logging side, CloudWatch automatically created log groups for each service: `/aws/lambda/openalex-ingestion` for the Lambda function, `/aws-glue/jobs/output` for the Glue ETL output, `/aws-glue/jobs/error` for errors, and `/aws/kinesisfirehose/firehose-stream` for Firehose delivery (**Figure 20**). Drilling into the Lambda logs shows the full execution trace - the API date range, pagination progress ("Got 50 works, total: 200"), the final success response, and the REPORT line with duration (9.8s) and memory usage (78 of 128 MB) (**Figure 21**). The Glue logs are similarly detailed: raw row count, the full before/after schema, cleaned row count, output path confirmation, and DynamoDB logging duration (**Figure 22**).

For query performance, the Athena Recent Queries tab acts as a built-in monitoring dashboard - every query is listed with its status, run time, and data scanned (**Figure 23**). My queries consistently ran in under 1 second and scanned under 2 MB each. That is tiny, and it's because of Parquet and Snappy compression. Parquet is columnar, so Athena only reads the columns referenced in the SELECT clause rather than scanning entire rows. A query selecting 3 of 9 columns reads roughly a third of the data.

CloudWatch also tracks 282 numerical Glue job metrics automatically, including JVM heap usage, S3 bytes read/written, CPU system load, filesystem operations (**Figure 24, Figure 25**). These metrics exist without any configuration; they are published by the Glue service itself. The high-resolution data points may expire after ~15 days, but the log data persists indefinitely under the default retention policy.

---

## Figures (Task 2)

![Figure 6: border-crossing-crawler configuration](screenshots/task2-p6-crawler-border-config.png)

![Figure 7: Both crawlers with status Succeeded](screenshots/task2-p6-crawlers-success.png)

![Figure 9: border_crossing table schema](screenshots/task2-p6-border-schema.png)

![Figure 10: openalex table schema](screenshots/task2-p6-openalex-schema.png)

![Figure 11: Athena query editor with data_warehouse](screenshots/task2-p7-athena-setup.png)

![Figure 12: Yearly border crossings trend](screenshots/task2-p7-query1-yearly-trend.png)

![Figure 13: Top 10 busiest border crossing ports](screenshots/task2-p7-query2-top-ports.png)

![Figure 14: Top 20 most-cited OpenAlex papers](screenshots/task2-p7-query3-openalex-cited.png)

![Figure 15: Yearly crossings split by border](screenshots/task2-p7-query4-multidim.png)

![Figure 16: DESCRIBE border_crossing showing schema-on-read](screenshots/task2-p7-schema-on-read.png)

![Figure 17: SHOW CREATE TABLE output](screenshots/task2-p7-show-create-table.png)

![Figure 18: Athena Recent Queries](screenshots/task2-p7-query-history.png)

![Figure 19: CloudWatch Log groups](screenshots/task2-p8-log-groups.png)

![Figure 21: Lambda execution logs](screenshots/task2-p8-lambda-logs.png)

![Figure 22: Glue ETL logs](screenshots/task2-p8-glue-logs.png)

![Figure 22: CloudWatch Glue job metrics](screenshots/task2-p8-metrics-glue-namespace.png)

![Figure 23: CloudWatch Metrics page](screenshots/task2-p8-cloudwatch-full.png)

---

# Task 3 - Workflow Automation & Visualisation


## 1. Extending the Step Functions workflow

The Task 1 pipeline stopped after the Glue ETL jobs wrote cleaned Parquet to S3. That meant every time I wanted to query the latest data in Athena, I had to manually run the crawlers first to update the catalog, then go to Athena and run the query myself. For Task 3 I extended the same state machine to automate all of that, so the pipeline goes from raw ingestion all the way through to a queryable CSV result without me having to touch anything in between.

The extended workflow adds three new states after the parallel ETL jobs. First, **RunCrawlersInParallel** triggers both Glue Crawlers at the same time so the Data Catalog picks up any schema or partition changes from the freshly written Parquet. Second, **WaitForCrawlers** pauses for 60 seconds to give the crawlers time to finish, since crawlers don't support `.sync` integration the way Glue jobs do. Third, **RunAthenaQuery** executes an aggregation query and writes the result as a CSV to `s3://ae1-data-lake-catliugit/athena-results/` (**Figure 26**). The full state machine definition is saved in `task3-visualisation/step-functions/workflow-definition.json`.

I kept the same error-handling approach from Task 1. Every new state has a `Catch` block that routes to `PipelineFailed`, so if a crawler can't find its target path or if the Athena query has a syntax error, the whole execution stops and I can see exactly which state failed in the graph. One thing that saved me a lot of effort was using `.sync` integration for the Athena step. Without it, I would have had to build a polling loop myself (a Wait state, then a GetQueryExecution call, then a Choice state to check if it's still running, then loop back). The `.sync` option handles all of that automatically, which kept the state machine definition clean.

The 60-second wait for crawlers is a bit of a workaround. In an ideal setup I would poll the crawler status the same way `.sync` polls Athena, but the Glue Crawler API doesn't support that integration pattern in Step Functions. Sixty seconds is enough for both crawlers to finish on a dataset this size, but for a much larger dataset I would probably need to increase it or build an actual polling loop.

After re-running the extended pipeline, the execution graph showed all states green (Lambda, both Glue jobs, both crawlers, the wait, and the Athena query), completing in about 3 minutes 52 seconds (**Figure 27**). The Athena CSV appeared in S3 immediately, confirming the full chain works end-to-end without manual intervention (**Figure 28**).

## 2. Athena query design

The query that the workflow runs is shaped specifically for downstream consumption:

```sql
SELECT state, border, measure, SUM(value) AS total_crossings
FROM data_warehouse.border_crossing
GROUP BY state, border, measure
ORDER BY total_crossings DESC
```

This produces 114 rows, one per unique combination of state, border type, and transport measure, with a `total_crossings` column holding the aggregate sum. I chose this level of granularity on purpose. Indexing all 273,000 raw rows into a visualisation tool would be slow and wouldn't add much insight, since most charts aggregate anyway. By pre-aggregating in Athena, I get a compact result set that's fast to load while still being granular enough to slice by state, by border, or by measure type depending on what question I'm trying to answer.

The query ran in under 600 ms and scanned roughly 1 MB (**Figure 29**), consistent with the Parquet performance I documented in Task 2. One thing worth noting: the result CSV is saved to the same `athena-results/` prefix where Athena stores all ad-hoc query results from the console. This means the Step Functions workflow and manual console queries share the same output location, which keeps things simple but does mean the folder accumulates files over time. In a production setup I would probably partition the output by date or add a cleanup policy.

## 3. Monitoring and observability

The extended workflow benefits from the same monitoring setup I described in Task 2, with no additional configuration needed. Step Functions automatically logs every state transition in its execution history, including timestamps, input/output payloads, and duration for each state. If the Athena query fails, I can click on the RunAthenaQuery state in the execution view and see the exact error message Athena returned.

On the CloudWatch side, the crawlers write to `/aws-glue/crawlers` and the Athena query appears in the Athena Recent Queries tab with its run time and data scanned. The Lambda and Glue ETL logs continue to flow into the same log groups documented in Task 2 (`/aws/lambda/openalex-ingestion`, `/aws-glue/jobs/output`). This means I have one place to check for pipeline health across all three tasks, rather than needing to set up separate monitoring for each extension.

The DynamoDB metadata table from Task 1 also still captures every Glue job run, so I have a persistent log of when each ETL job executed, how many rows it processed, and how long it took. Combined with the Step Functions execution history and CloudWatch logs, this gives me full traceability from the initial API call through to the final Athena result.

---

## Figures (Task 3)

![Figure 26: Extended Step Functions workflow with crawlers and Athena](screenshots/task3-p9-workflow-extended.png)

![Figure 27: Successful execution of extended pipeline with duration and details](screenshots/task3-p9-execution-success.png)

![Figure 28: S3 showing Athena result CSV in athena-results/](screenshots/task3-p9-athena-export.png)

![Figure 29: Athena query results showing 114 rows with state, border, measure, total_crossings](screenshots/task3-p9-athena-query-results.png)
