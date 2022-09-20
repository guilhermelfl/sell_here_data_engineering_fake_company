
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT 
reseller_cnpj, 
reseller_name, 
agreed_rate_pct, 
creation_date
FROM {{source('sell_here','reseller')}}

{% if is_incremental() %}
  where creation_date > (select max(creation_date) from {{ this }})
{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
