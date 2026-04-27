Welcome to the nbp_pipeline mini project,
containing pyspark scripts for production-ready NBP data ingestion, handling and analysis.
Entire project has been completed within Databricks platform, this repo is synchronized.
In order to access business dashboard, containing exchange rate dynamics and other requested insights, please use the following link:
https://dbc-70ccf849-562e.cloud.databricks.com/dashboardsv3/01f1424810de1906a89213b5d16a9c2d/published?o=7474652986701805
In case of any issues, I also attach the PDF copy of the dashboard here in repo.

The project is example of classical medallion architecture - bronze layer ingests data from external system
(NBP api) as is, with the use of incremental loading to avoid unnecessary waste of memory space in case of multiple runs per day.

Silver layer takes data from bronze layer, narrowing scope on only interesting columns and doing deduplication.

Finally gold layer answers the question of business users and it is done in SQL as SQL is more familiar to business users.

As it is simple pipeline, Data dashboard containing visualization and data insights, builds on top of silver layer.
