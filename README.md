## Download data from: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page 
MY LAKEHOUSE: Project_lakehouse

<img width="1366" height="339" alt="files1" src="https://github.com/user-attachments/assets/d38e03b2-dc2e-4a6c-9d0a-d0549cac1bbf" />
<img width="1077" height="294" alt="files 2" src="https://github.com/user-attachments/assets/c8848dec-7946-4a51-a156-fb350c586881" />

MY WAREHOUSE: Project_Warehouse

<img width="332" height="254" alt="dbo" src="https://github.com/user-attachments/assets/3df55d95-8e55-4c44-8bb9-1a86ba0dc87f" />
<img width="360" height="326" alt="metadata" src="https://github.com/user-attachments/assets/7f03752e-3d9f-4893-b908-dcc2dfe5f19e" />
<img width="291" height="226" alt="stg" src="https://github.com/user-attachments/assets/ba8fc350-3a20-4d1a-b201-bc19b627abb3" />


## 01. Code I used for Pipeline: pl_stg_processing_nyctaxi
<img width="1366" height="768" alt="PIPELINE 1" src="https://github.com/user-attachments/assets/982fc94d-26b6-44f0-9d6d-2e1b99e9be73" />

***Copy to Staging***

Pre Copy Script--- 

```bash
delete from stg.nyctaxi_yellow
```

***v_end_date***
Pipeline expression for v_end_date Set Variable activity

```bash
@addToTime(concat(variables('v_date'), '-01'),1,'Month')
```
<img width="1304" height="514" alt="4" src="https://github.com/user-attachments/assets/4dbd0445-c9dc-42ee-893a-15db0d39e75e" />


***SP Removing Outlier Dates***

For the Stored Procedure Activity “SP Removing Outlier Dates”.

Create the Stored Procedure stg.data_cleaning_stg in the Data Warehouse using the code below.

```bash
create procedure stg.data_cleaning_stg
@end_date datetime2,
@start_date datetime2
as
delete from stg.nyctaxi_yellow where tpep_pickup_datetime < @start_date or tpep_pickup_datetime > @end_date;
```
<img width="975" height="266" alt="image" src="https://github.com/user-attachments/assets/82df0a29-d1b7-4cb2-bcea-93cf6ec997e5" />

***SP Loading Staging Metadata***

For the Stored Procedure Activity “SP Loading Staging Metadata”.

Code to create the metadata.processing_log table.

```bash
create schema metadata;
```

```bash
create table metadata.processing_log
(
	pipeline_run_id varchar(255), 
	table_processed varchar(255), 
	rows_processed INT, 
	latest_processed_pickup datetime2(6),
	processed_datetime datetime2(6)
);
```

```bash
Created the Stored Procedure metadata.insert_staging_metadata in the Data Warehouse using the code below.
CREATE PROCEDURE metadata.insert_staging_metadata
    @pipeline_run_id VARCHAR(255),
    @table_name VARCHAR(255),
    @processed_date DATETIME2
AS
    INSERT INTO metadata.processing_log (pipeline_run_id, table_processed, rows_processed, latest_processed_pickup, processed_datetime)
    SELECT
        @pipeline_run_id AS pipeline_id,
        @table_name AS table_processed,
        COUNT(*) AS rows_processed,
        MAX(tpep_pickup_datetime) AS latest_processed_pickup,
        @processed_date AS processed_datetime
    FROM stg.nyctaxi_yellow;
```

<img width="975" height="303" alt="image" src="https://github.com/user-attachments/assets/8c9be7fd-3cec-449f-b017-09e38e273101" />

***Latest Processed Data***

For the Script Activity “Latest Processed Data”

```bash
select top 1 
latest_processed_pickup 
from metadata.processing_log 
where table_processed = 'staging_nyctaxi_yellow'
order by latest_processed_pickup desc;
```

<img width="447" height="413" alt="51Latest Processed data ka output 8SP metadata idea (jo red mein hai wo system mein fixed hota hai)" src="https://github.com/user-attachments/assets/b6e1bbe2-e9e9-4dec-9897-8d60207c13a2" />

***v_date***

Pipeline expression for v_date Set Variable activity

```bash
@formatDateTime(addToTime(activity('Latest Processed Date').output.resultSets[0].rows[0].latest_processed_pickup, 1, 'Month'), 'yyyy-MM')
```

## 02. Code used in Pipeline: pl_pres_processing_nyctaxi

<img width="1366" height="768" alt="pipeline 2" src="https://github.com/user-attachments/assets/cac477f3-a00a-47b6-9201-4e31b5a13604" />

***Create the dbo.nyctaxi_yellow table***

This is the initial empty table so we can load the data from the Dataflow/Stored Procedure acivities
```bash
CREATE TABLE dbo.nyctaxi_yellow
(
	vendor varchar(50),
	tpep_pickup_datetime date,
	tpep_dropoff_datetime date,
	pu_borough varchar(100),
	pu_zone varchar(100),
	do_borough varchar(100),
	do_zone varchar(100),
	payment_method varchar(50),
	passenger_count int,
	trip_distance FLOAT,
	total_amount FLOAT
);
```
***SP Processing Presentation***

For the Stored Procedure Activity “SP Processing Presentation ”.

Create the Stored Procedure dbo.process_presentation in the Data Warehouse using the code below.

```bash
CREATE PROCEDURE dbo.process_presentation
AS
BEGIN

    INSERT INTO dbo.nyctaxi_yellow
    (
        vendor,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        pu_borough,
        pu_zone,
        do_borough,
        do_zone,
        payment_method,
        passenger_count,
        trip_distance,
        total_amount
    )
    SELECT
        CASE 
            WHEN nty.VendorID = 1 THEN 'Creative Mobile Technologies'
            WHEN nty.VendorID = 2 THEN 'VeriFone'
            ELSE 'Unknown'
        END AS vendor,

        FORMAT(nty.tpep_pickup_datetime, 'yyyy-MM-dd') AS tpep_pickup_datetime,
        FORMAT(nty.tpep_dropoff_datetime, 'yyyy-MM-dd') AS tpep_dropoff_datetime,

        lu1.Borough AS pu_borough,
        lu1.Zone AS pu_zone,
        lu2.Borough AS do_borough,
        lu2.Zone AS do_zone,

        CASE 
            WHEN nty.payment_type = 1 THEN 'Credit Card'
            WHEN nty.payment_type = 2 THEN 'Cash'
            WHEN nty.payment_type = 3 THEN 'No Charge'
            WHEN nty.payment_type = 4 THEN 'Dispute'
            WHEN nty.payment_type = 5 THEN 'Unknown'
            WHEN nty.payment_type = 6 THEN 'Voided Trip'
            ELSE 'Unknown'
        END AS payment_method,

        nty.passenger_count,
        nty.trip_distance,
        nty.total_amount
    FROM stg.nyctaxi_yellow nty
    LEFT JOIN stg.nyctaxi_zone_lookup lu1
        ON nty.PULocationID = lu1.LocationID
    LEFT JOIN stg.nyctaxi_zone_lookup lu2
        ON nty.DOLocationID = lu2.LocationID;

END;
GO
```
***SP Loading Presentation Metadata***

For the Stored Procedure Activity “SP Loading Presentation Metadata”.

Create the Stored Procedure metadata.insert_presenatation_metadata in the Data Warehouse using the code below.
```bash
CREATE PROCEDURE metadata.insert_presentation_metadata
    @pipeline_run_id VARCHAR(255),
    @table_name VARCHAR(255),
    @processed_date DATETIME2
AS
    INSERT INTO metadata.processing_log (pipeline_run_id, table_processed, rows_processed, latest_processed_pickup, processed_datetime)
    SELECT
        @pipeline_run_id AS pipeline_id,
        @table_name AS table_processed,
        COUNT(*) AS rows_processed,
        MAX(tpep_pickup_datetime) AS latest_processed_pickup,
        @processed_date AS processed_datetime
    FROM dbo.nyctaxi_yellow;
```
<img width="975" height="328" alt="image" src="https://github.com/user-attachments/assets/c0ea0d99-1569-4a90-b03a-28ba16a1e98a" />

***All pipleines in 1 single folder:***

<img width="1366" height="253" alt="pipelines folder" src="https://github.com/user-attachments/assets/76249e58-d374-4f11-b23a-3c914a2541de" />

***Final Invoking pipelines:***

<img width="1366" height="768" alt="final pipeline" src="https://github.com/user-attachments/assets/bd0f1a88-e65e-4229-b6ec-e2fc3369a911" />

## BI Reporting






