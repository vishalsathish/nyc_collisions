# nyc_collisions

# **End-to-End Data Pipeline with NYC Motor Vehicle Collisions**

Goal is to extract the data via API
Transform the data:

**Bronze** - loading zone and raw ingestion

**Silver** - filtered and cleaned

**Gold** - business ready and production grade

<img width="793" height="516" alt="image" src="https://github.com/user-attachments/assets/b7110673-561e-417c-acc2-a7f21edcde33" />


**Database Queries**

<img width="1303" height="596" alt="image" src="https://github.com/user-attachments/assets/6bcfb10d-74b3-4364-b7c8-30906d4c9705" />


**Bronze Layer Lessons Learned:**
Batch processing and pagination - getting bits of the data at a time through chunks at a time instead of all at once.

Integrated with NYC Open Data API using requests

Fixed API issues from incorrect endpoint and handled 403 errors using headers (User-Agent)

Parsed API response into structured data format using pandas

Pagination → implemented offset-based pagination using $limit and $offset. Built a loop to continuously fetch batches and stop when no more data is being collected

Used incremental loading through the $where clause on crash_date and shifted away from full data loads to daily ingestion

Saved data as parquet files for efficient columnar storage and implemented append-only batch ingestion while avoiding memory issues through writing batches directly to disk instead of storing in memory.

Used Hive style partitioning for faster reads and scalable storage

**Silver Layer Lessons Learned:**

