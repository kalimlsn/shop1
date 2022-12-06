{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_coupons') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['code'], ['code']) }} as code,
    {{ json_extract('table_alias', '_airbyte_data', ['_links']) }} as _links,
    {{ json_extract_scalar('_airbyte_data', ['amount'], ['amount']) }} as amount,
    {{ json_extract_array('_airbyte_data', ['used_by'], ['used_by']) }} as used_by,
    {{ json_extract_scalar('_airbyte_data', ['shop_url'], ['shop_url']) }} as shop_url,
    {{ json_extract('table_alias', '_airbyte_data', ['meta_data']) }} as meta_data,
    {{ json_extract_scalar('_airbyte_data', ['description'], ['description']) }} as description,
    {{ json_extract_array('_airbyte_data', ['product_ids'], ['product_ids']) }} as product_ids,
    {{ json_extract_scalar('_airbyte_data', ['usage_count'], ['usage_count']) }} as usage_count,
    {{ json_extract_scalar('_airbyte_data', ['usage_limit'], ['usage_limit']) }} as usage_limit,
    {{ json_extract_scalar('_airbyte_data', ['date_created'], ['date_created']) }} as date_created,
    {{ json_extract_scalar('_airbyte_data', ['date_expires'], ['date_expires']) }} as date_expires,
    {{ json_extract_scalar('_airbyte_data', ['date_modified'], ['date_modified']) }} as date_modified,
    {{ json_extract_scalar('_airbyte_data', ['discount_type'], ['discount_type']) }} as discount_type,
    {{ json_extract_scalar('_airbyte_data', ['free_shipping'], ['free_shipping']) }} as free_shipping,
    {{ json_extract_scalar('_airbyte_data', ['individual_use'], ['individual_use']) }} as individual_use,
    {{ json_extract_scalar('_airbyte_data', ['maximum_amount'], ['maximum_amount']) }} as maximum_amount,
    {{ json_extract_scalar('_airbyte_data', ['minimum_amount'], ['minimum_amount']) }} as minimum_amount,
    {{ json_extract_scalar('_airbyte_data', ['date_created_gmt'], ['date_created_gmt']) }} as date_created_gmt,
    {{ json_extract_scalar('_airbyte_data', ['date_expires_gmt'], ['date_expires_gmt']) }} as date_expires_gmt,
    {{ json_extract_scalar('_airbyte_data', ['date_modified_gmt'], ['date_modified_gmt']) }} as date_modified_gmt,
    {{ json_extract_array('_airbyte_data', ['email_restrictions'], ['email_restrictions']) }} as email_restrictions,
    {{ json_extract_scalar('_airbyte_data', ['exclude_sale_items'], ['exclude_sale_items']) }} as exclude_sale_items,
    {{ json_extract_array('_airbyte_data', ['product_categories'], ['product_categories']) }} as product_categories,
    {{ json_extract_array('_airbyte_data', ['excluded_product_ids'], ['excluded_product_ids']) }} as excluded_product_ids,
    {{ json_extract_scalar('_airbyte_data', ['usage_limit_per_user'], ['usage_limit_per_user']) }} as usage_limit_per_user,
    {{ json_extract_scalar('_airbyte_data', ['limit_usage_to_x_items'], ['limit_usage_to_x_items']) }} as limit_usage_to_x_items,
    {{ json_extract_array('_airbyte_data', ['excluded_product_categories'], ['excluded_product_categories']) }} as excluded_product_categories,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_coupons') }} as table_alias
-- coupons
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

