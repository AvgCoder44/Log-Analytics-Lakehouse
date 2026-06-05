# Log-Analytics-Lakehouse
a data engineering pipeline that takes raw server log files, processes them through three increasingly refined layers, and produces analytical insights at the end. The entire thing runs on Databricks using PySpark and Delta Lake, following the industry-standard Medallion Architecture pattern.
The data source layer
Apache access logs are the raw input. These are standard web server logs that every server generates automatically. A single line looks like this:
192.168.1.1 - - [06/Jun/2026:10:23:45] "GET /api/users HTTP/1.1" 200 1234
It contains an IP address, timestamp, HTTP method, endpoint, status code, and response size — all jammed into one unstructured string. You also have AWS S3 as a source because logs from multiple servers get collected and stored there, and custom collectors which are agents running on servers that ship logs continuously.
The challenge is that all three sources dump data in slightly different formats, at different frequencies, and with different field structures. Your pipeline's first job is to handle all of them uniformly.

Bronze layer — raw ingestion
What it does: Takes everything coming from the sources and lands it into a Delta table as-is, with minimal transformation. Think of it as a raw landing zone.
How it works technically:
PySpark's Auto Loader watches the incoming data location and picks up new files automatically as they arrive — you don't have to manually trigger anything. It reads the raw log lines and writes them directly into a Delta table.
pythondf = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "text") \
    .load("/data/raw/apache-logs/")

df.writeStream.format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .start("/delta/bronze/logs")
Schema-on-read means you're not enforcing any structure at this point — you store the raw string and figure out the structure later. This is intentional. If the source format changes, you don't lose data because you always have the original.
Why this maps to your real work: This is exactly what you did with Logstash and Vector.dev at Blusapphire — ingesting from Filebeat, Winlogbeat, S3, and custom collectors into a central store. Same concept, different tool.

Silver layer — transform and validate
What it does: Takes the raw Bronze data, parses it into structured fields, applies data quality checks, and writes clean structured records into a new Delta table.
How it works technically — parsing:
pythonfrom pyspark.sql.functions import regexp_extract, col, to_timestamp

log_pattern = r'(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+)[^"]*" (\d+) (\d+)'

silver_df = bronze_df.select(
    regexp_extract('value', log_pattern, 1).alias('ip_address'),
    to_timestamp(regexp_extract('value', log_pattern, 2), 
                 'dd/MMM/yyyy:HH:mm:ss').alias('timestamp'),
    regexp_extract('value', log_pattern, 3).alias('http_method'),
    regexp_extract('value', log_pattern, 4).alias('endpoint'),
    regexp_extract('value', log_pattern, 5).cast('int').alias('status_code'),
    regexp_extract('value', log_pattern, 6).cast('int').alias('bytes_sent')
)
This is your parser work from the internship — same logic, same thinking, just written in PySpark instead of VRL or Logstash DSL.
Data quality checks — this is the key part:
pythonfrom pyspark.sql.functions import col

quality_df = silver_df.filter(
    col('ip_address').isNotNull() &
    col('timestamp').isNotNull() &
    col('status_code').between(100, 599) &
    col('bytes_sent') >= 0
)
This directly maps to the validation framework you built at Blusapphire that flagged null violations and type mismatches. Same concept.
Schema enforcement means you define what the Silver table must look like and reject anything that doesn't conform — so downstream consumers always get consistent data.

Gold layer — aggregations for analytics
What it does: Takes the clean Silver data and computes business-level metrics that someone can actually make decisions from.
How it works technically:
pythonfrom pyspark.sql.functions import hour, count, when

# Error rate by hour
error_by_hour = silver_df.groupBy(hour('timestamp').alias('hour')) \
    .agg(
        count('*').alias('total_requests'),
        count(when(col('status_code') >= 400, 1)).alias('error_count')
    ) \
    .withColumn('error_rate', col('error_count') / col('total_requests'))

# Top source IPs
top_ips = silver_df.groupBy('ip_address') \
    .count() \
    .orderBy('count', ascending=False) \
    .limit(10)
This runs across 500K+ daily log records and produces small, fast, analytical tables that can power dashboards or be queried directly with SQL.

Delta Lake optimisations
After writing the Gold tables you apply two key optimisations:
Date partitioning — organises data on disk by date so when someone queries "show me errors from last Tuesday" Spark only reads that day's files instead of scanning everything:
pythondf.write.format("delta") \
    .partitionBy("date") \
    .mode("overwrite") \
    .save("/delta/gold/error_rates")
Z-ordering on IP address — co-locates records with the same IP address on the same data files so queries filtering by IP are dramatically faster:
pythonspark.sql("OPTIMIZE delta.`/delta/gold/error_rates` ZORDER BY (ip_address)")

Git and PR workflow
Every notebook — Bronze ingestion, Silver transformation, Gold aggregation — lives in a GitHub repo. Changes go through pull requests before merging to main. This mirrors the PR-based code review process you follow at Blusapphire right now.
