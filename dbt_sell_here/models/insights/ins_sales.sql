
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with first_step as (
                    SELECT 
                      sal.sales_id, 
                      sal.sales_datetime, 
                      sal.reseller_cnpj, 
                      sal.product_id, 
                      sal.customer_cpf, 
                      sal.transaction_amount,
                      case when sal.sale_type = 'Website' then res.agreed_rate_pct * sal.transaction_amount
                          when sal.sale_type = 'External' then res.agreed_rate_pct * sal.transaction_amount * 0.1
                      end as transaction_charge_amount,
                      sal.payment_type, 
                      sal.creation_date,
                      sal.sale_type,
                      res.reseller_name,
                      prod.product_name,
                      prod.product_category,
                      cust.customer_state
                    FROM {{ref('int_sales')}} as sal
                    left join {{ref('stg_reseller')}} as res on sal.reseller_cnpj = res.reseller_cnpj
                    left join {{ref('stg_products')}} as prod on sal.product_id = prod.product_id
                    left join {{ref('stg_customers')}} as cust on sal.customer_cpf = cust.customer_cpf
                    )
select *, transaction_amount - transaction_charge_amount as reseller_amount from first_step



{% if is_incremental() %}
  where creation_date > (select max(creation_date) from {{ this }})
{% endif %}
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
