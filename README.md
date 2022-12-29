### Udacity Project: Sparkify Data Lake Project with Apache Spark
Building an ETL pipeline with the ability to extract song and log data from an S3 bucket, process the data using Spark, and load the data back into S3 as a series of dimensional tables in spark parquet files is the goal of this project. This aids analysts in their ongoing quest to gain knowledge about what their users are listening to.

### Design of database schema
One fact table, "songplays," four-dimensional tables for users, songs, artists, and time are among the developed tables. According to the star schema idea, this will have clear data that is appropriate for OLAP (Online Analytical Processing) operations, which the analysts will need to run in order to obtain the insights they seek.

### ETL Pipline
To fit the data model in the intended destination tables, the extracted data needs to be modified. For instance, converting unix-format source data to timestamp is necessary so that the year, month, day, and hour values, among other data, may be retrieved and placed in the appropriate destination time and songplays table columns. Duplicates must also be taken into account by the script in order to prevent them from being included in the final data that is loaded into the tables.

### Dataset
The datasets required are in JSON format and were obtained from an S3 bucket. "Log data" and "song data" are the two datasets. While the "log data" dataset contains created log files based on the songs in "song data," the "song data" dataset from [Million Song Dataset](http://millionsongdataset.com/). 

### Project File
**dl.cfg:  contain AWS keys.
**elt.py: executed retrieves the song and log data in the s3 bucket, transforms the data into fact and dimensional tables then loads the table data back into s3 as parquet files.
**README.md: Project informations