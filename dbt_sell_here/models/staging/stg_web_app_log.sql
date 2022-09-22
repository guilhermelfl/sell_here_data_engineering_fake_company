
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT 
client_ip,
identifier,
user_name,
cast(log_date_time as timestamp) as log_date_time, 
replace(request_line,'"','') as request_line,
server_response, 
cast(response_size_in_bytes as int) as response_size_in_bytes, 
replace(referer,'"','') as referer,
user_agent
FROM {{source('external_data','web_app_logs')}}

{% if is_incremental() %}
  where creation_date > (select max(creation_date) from {{ this }})
{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
