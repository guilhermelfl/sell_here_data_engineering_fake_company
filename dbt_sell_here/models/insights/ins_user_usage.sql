
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with first_step as (
                    SELECT 
                      wal.user_name,
                      wal.client_ip,
                      wal.request_line,
                      wal.server_response,
                      wal.response_size_in_bytes,
                      wal.referer,
                      wal.user_agent,
                      cust.customer_state
                    FROM {{ref('stg_web_app_log')}} as wal
                    left join {{ref('stg_customers')}} as cust on wal.user_name = cust.customer_user_name
                    )
select * from first_step



{% if is_incremental() %}
  where log_date_time > (select max(log_date_time) from {{ this }})
{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
