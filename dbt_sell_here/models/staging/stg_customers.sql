
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT 
customer_cpf, 
customer_name, 
customer_address, 
right(customer_address,2) as customer_state, 
customer_email, 
birth_date, 
creation_date
FROM {{source('sell_here','customers')}}

{% if is_incremental() %}
  where creation_date > (select max(creation_date) from {{ this }})
{% endif %}

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
