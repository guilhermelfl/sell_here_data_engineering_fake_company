
# Sell Here

#### Data Engineering project

## What is sell here?

Sell Here is a fake web platform to help companies sell their products through sell_here website.

Also receiving external sales to help you with insights and analytics for 0.1 * the percentage agreed rate for the website sales.


## Sell here fake data

### Postgres

Postgres is the choosen company's database. This way we will have a database schema with 5 tables with the company's internal generated data.

Sales made from the sell_here website charge the reseller the `agreed_rate_pct` on the `transaction_amount` value.

1. Customers, this table contains customers information

|Customer|
| ------ |
|customer_cpf|
|customer_user_name|
|customer_name|
|customer_address|
|customer_email|
|birth_date|
|creation_date|


2. Products, this table contains products information

|Products|
| ------ |
|product_id|
|product_name|
|base_product_price|
|product_category|
|creation_date|

3. Reseller, this table contains resellers information

|Reseller|
| ------ |
|reseller_cnpj|
|reseller_name|
|agreed_rate_pct|
|creation_date|

4. Reseller Products, this table contains what product which reseller sells and their information

|Reseller_Products|
| ------ |
|reseller_cnpj|
|product_id|
|reseller_price|
|creation_date|

5. Sales, this table contains sales information

|Sales|
| ------ |
|sales_id|
|sales_datetime|
|reseller_cnpj|
|product_id|
|customer_cpf|
|transaction_amount|
|payment_type|
|creation_date|


To set up this database we can initialize it with `docker-compose up -d` via terminal from *Docker* folder. Docker will create a container based on `docker-compose.yml` and a run `1-init.sql` creating our schemas and tables. 



### External sales

External sales are files sell here receive to add them to our analytics insights, this file is not mandatory.

Because of the infra used to ingest, maintain... this external sales sell here charge `0.1 * agreed_percentage_rate` on the external sales.

These files consist in the same idea as postgres sales table, giving us the basic information of each item sold.

|file columns|
| ------ |
|sales_id|
|sales_datetime|
|reseller_cnpj|
|product_id|
|customer_cpf|
|transaction_amount|
|payment_type|
|creation_date|



## Generating fake data

To generate the fake data there is a python script inside `faking_data` folder, which will generate our postgres business data and ingest it. 
This script will also create the extenal files inside `reseller_files` script generated folder along the files with the name patterns `(DATE)_(RESELLER-CNPJ).xlsx`.



### Ingesting the external files

These Excel external data files are ingested by the script `files_ingestion.py` which will read every file inside the `reseller_files` folder, ingest it to our datawarehouse inside the `external_reseller_sales` table, and after that deleting the file from the folder.

## Data Warehouse

Our data warehouse in this project is inside the `dw` schema on postgres.

To deal with transformations and business rules [DBT](https://www.getdbt.com/) was the choosen tool. Its project is located inside `dbt_sell_here` folder.

After the pipeline run we can get to our final layer `ins_sales`, table with some joins and transformations to get insights from.