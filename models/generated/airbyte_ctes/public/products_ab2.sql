{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('products_ab1') }}
select
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast(tags as {{ dbt_utils.type_string() }}) as tags,
    cast(image as {{ type_json() }}) as image,
    cast(title as {{ dbt_utils.type_string() }}) as title,
    cast(handle as {{ dbt_utils.type_string() }}) as handle,
    images,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(vendor as {{ dbt_utils.type_string() }}) as vendor,
    {{ adapter.quote('options') }},
    cast(shop_url as {{ dbt_utils.type_string() }}) as shop_url,
    variants,
    cast(body_html as {{ dbt_utils.type_string() }}) as body_html,
    cast({{ empty_string_to_null('created_at') }} as {{ type_timestamp_with_timezone() }}) as created_at,
    cast({{ empty_string_to_null('updated_at') }} as {{ type_timestamp_with_timezone() }}) as updated_at,
    cast(product_type as {{ dbt_utils.type_string() }}) as product_type,
    cast({{ empty_string_to_null('published_at') }} as {{ type_timestamp_with_timezone() }}) as published_at,
    cast(published_scope as {{ dbt_utils.type_string() }}) as published_scope,
    cast(template_suffix as {{ dbt_utils.type_string() }}) as template_suffix,
    cast(admin_graphql_api_id as {{ dbt_utils.type_string() }}) as admin_graphql_api_id,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('products_ab1') }}
-- products
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

