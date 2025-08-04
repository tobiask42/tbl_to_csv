# TPC-H tbl2csv Converter

Ein lernorientiertes CLI-Tool zur Konvertierung großer `.tbl`-Dateien des TPC-H Benchmarks in CSV. 
Ziel: Performante Umsetzung eines skalierbaren ETL-Konzepts mit Python, Polars und Spark.

## Features
- Automatischer Backend-Switch (Polars <-> Spark)
- Partitioniertes Schreiben + Merge (Spark)
- Fortschrittsanzeige (tqdm), Logging (loguru)
- Unit- und Integrationstests mit pytest