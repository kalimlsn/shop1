{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('public', '_airbyte_raw_products') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as {{ adapter.quote('id') }},
    {{ json_extract_scalar('_airbyte_data', ['tags'], ['tags']) }} as tags,
    {{ json_extract('table_alias', '_airbyte_data', ['image'], ['image']) }} as image,
    {{ json_extract_scalar('_airbyte_data', ['title'], ['title']) }} as title,
    {{ json_extract_scalar('_airbyte_data', ['handle'], ['handle']) }} as handle,
    {{ json_extract_array('_airbyte_data', ['images'], ['images']) }} as images,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as status,
    {{ json_extract_scalar('_airbyte_data', ['vendor'], ['vendor']) }} as vendor,
    {{ json_extract_array('_airbyte_data', ['options'], ['options']) }} as {{ adapter.quote('options') }},
    {{ json_extract_scalar('_airbyte_data', ['shop_url'], ['shop_url']) }} as shop_url,
    {{ json_extract_array('_airbyte_data', ['variants'], ['variants']) }} as variants,
    {{ json_extract_scalar('_airbyte_data', ['body_html'], ['body_html']) }} as body_html,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['product_type'], ['product_type']) }} as product_type,
    {{ json_extract_scalar('_airbyte_data', ['published_at'], ['published_at']) }} as published_at,
    {{ json_extract_scalar('_airbyte_data', ['published_scope'], ['published_scope']) }} as published_scope,
    {{ json_extract_scalar('_airbyte_data', ['template_suffix'], ['template_suffix']) }} as template_suffix,
    {{ json_extract_scalar('_airbyte_data', ['admin_graphql_api_id'], ['admin_graphql_api_id']) }} as admin_graphql_api_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('public', '_airbyte_raw_products') }} as table_alias
-- products
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

