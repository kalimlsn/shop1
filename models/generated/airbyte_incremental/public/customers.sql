{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('customers_scd') }}
select
    _airbyte_unique_key,
    {{ adapter.quote('id') }},
    note,
    tags,
    email,
    phone,
    {{ adapter.quote('state') }},
    currency,
    shop_url,
    addresses,
    last_name,
    created_at,
    first_name,
    tax_exempt,
    updated_at,
    total_spent,
    orders_count,
    last_order_id,
    verified_email,
    default_address,
    last_order_name,
    accepts_marketing,
    admin_graphql_api_id,
    multipass_identifier,
    accepts_marketing_updated_at,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_customers_hashid
from {{ ref('customers_scd') }}
-- customers from {{ source('public', '_airbyte_raw_customers') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

