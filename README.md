# Building Data Lake for modern analytics

This project involves extracting data from a food service app, cleaning it, and converting it into a format that is easier to analyze. The data can help identify popular dishes, analyze customer sentiment, and identify gaps in the market. 
***
## Libraries:

- Beautiful soup
- SQLAlchemy
- Spark
- SparkSQL
***
## Tools used:
- Python
- SQL server
- Ubuntu
- AWS (S3, Lambda, Glue, Athena) 
***
## Overview:

- Extracting data from website using beautiful soup.
- Storing the raw data into a SQL database (local storage).
- Uploading the raw data to AWS S3 bucket.
- Cleaning the data and converting it to a standard format.
- Loading the data in datamarts for end users.
***
## Architecture:
<br/>
<br/>
<img src="https://github.com/mithun-sudo/Building-Data-Lake-for-modern-analytics/blob/main/images/Architecture.png" width="700">
<br/>

***
## Project
- Create a database 'main' in SQL server in ubuntu environment (local system)
- In that database, three tables named 'restaurant_info', 'items' and 'reviews'.
- Using beautiful soup data were extracted from the website and loaded into SQL server.
- The tables had these initial schemas:
##### Restaurant info:

<br/>
<br/>
<img src="https://github.com/mithun-sudo/Building-Data-Lake-for-modern-analytics/blob/main/images/restaurant_schema.JPG" width="400">
<br/>

##### Food items:

<br/>
<br/>
<img src="https://github.com/mithun-sudo/Building-Data-Lake-for-modern-analytics/blob/main/images/item_schema.JPG" width="400">
<br/>

##### Reviews:

<br/>
<br/>
<img src="https://github.com/mithun-sudo/Building-Data-Lake-for-modern-analytics/blob/main/images/reviews.jpg" width="400">
<br/>


-The tables were converted into csv files using the command:

    SELECT * FROM restaurant_info INTO OUTFILE '/var/lib/mysql-files/restaurant_info.csv' FIELDS TERMINATED BY '|' ENCLOSED BY '"' LINES TERMINATED BY '\n';

- Delimiter '|' is used instead of comma because there were certain fields which had comma and that comma caused errors while reading the csv file.
- The csv files of the three tables are now sent to AWS S3 bucket 'scraped-data-landing-zone' through AWS CLI command.
- In AWS environment we set up a lambda function that gets triggered whenever a file with a suffix .csv lands on S3 bucket.
- Event trigger for PUT OBJECT / MULTIPART UPLOAD is assigned.
- Lambda function code:

        import awswrangler as wr
        import boto3


        def lambda_handler(event, context):
            bucket = event["Records"][0]["s3"]["bucket"]["name"]
            print(bucket)
            key = event["Records"][0]["s3"]["object"]["key"]
            print(key)
            key_list = key.split('/')
            file_name = key_list[len(key_list) - 1].split('.')[0]
            df = wr.s3.read_csv(f"s3://{bucket}/{key}", sep = '|')
            wr.s3.to_parquet(df, f"s3://transformation-1-zone/{key_list[0]}/{file_name}.parquet", compression='snappy')
            client = boto3.client('glue')
            client.start_crawler(Name='Scraped-data-crawler')

- The function coverts all the csv file that lands in 'scraped-data-landing-zone' to parquet format and stores it in another bucket called 'transformation-1-zone'.
- Then it calls the glue crawler 'Scraped-data-crawler' to crawl through 'transformation-1-zone' bucket and stores meta data in glue catalog.
- The data as it is, is of no use. Some data cleansing and proceesing has to be performed so that data driven decisions can be taken.

- Schema of table restaurant_info:

