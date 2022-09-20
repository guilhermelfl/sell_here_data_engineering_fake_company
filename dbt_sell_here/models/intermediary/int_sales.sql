
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

SELECT 
sales_id, 
sales_datetime, 
reseller_cnpj, 
product_id, 
customer_cpf, 
transaction_amount, 
payment_type, 
creation_date,
'External' as sale_type
FROM {{ref('stg_external_reseller_sales')}}

union all

SELECT 
sales_id, 
sales_datetime, 
reseller_cnpj, 
product_id, 
customer_cpf, 
transaction_amount, 
payment_type, 
creation_date,
'Website' as sale_type
FROM {{ref('stg_sales')}}



{% if is_incremental() %}
  where creation_date > (select max(creation_date) from {{ this }})
{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
