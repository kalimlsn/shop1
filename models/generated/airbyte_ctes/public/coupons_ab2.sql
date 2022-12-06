{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('coupons_ab1') }}
select
    cast({{ adapter.quote('id') }} as {{ dbt_utils.type_bigint() }}) as {{ adapter.quote('id') }},
    cast(code as {{ dbt_utils.type_string() }}) as code,
    _links,
    cast(amount as {{ dbt_utils.type_string() }}) as amount,
    used_by,
    cast(shop_url as {{ dbt_utils.type_string() }}) as shop_url,
    meta_data,
    cast(description as {{ dbt_utils.type_string() }}) as description,
    product_ids,
    cast(usage_count as {{ dbt_utils.type_bigint() }}) as usage_count,
    cast(usage_limit as {{ dbt_utils.type_bigint() }}) as usage_limit,
    cast({{ empty_string_to_null('date_created') }} as {{ type_timestamp_with_timezone() }}) as date_created,
    cast(date_expires as {{ dbt_utils.type_string() }}) as date_expires,
    cast({{ empty_string_to_null('date_modified') }} as {{ type_timestamp_with_timezone() }}) as date_modified,
    cast(discount_type as {{ dbt_utils.type_string() }}) as discount_type,
    {{ cast_to_boolean('free_shipping') }} as free_shipping,
    {{ cast_to_boolean('individual_use') }} as individual_use,
    cast(maximum_amount as {{ dbt_utils.type_string() }}) as maximum_amount,
    cast(minimum_amount as {{ dbt_utils.type_string() }}) as minimum_amount,
    cast({{ empty_string_to_null('date_created_gmt') }} as {{ type_timestamp_with_timezone() }}) as date_created_gmt,
    cast(date_expires_gmt as {{ dbt_utils.type_string() }}) as date_expires_gmt,
    cast({{ empty_string_to_null('date_modified_gmt') }} as {{ type_timestamp_with_timezone() }}) as date_modified_gmt,
    email_restrictions,
    {{ cast_to_boolean('exclude_sale_items') }} as exclude_sale_items,
    product_categories,
    excluded_product_ids,
    cast(usage_limit_per_user as {{ dbt_utils.type_bigint() }}) as usage_limit_per_user,
    cast(limit_usage_to_x_items as {{ dbt_utils.type_bigint() }}) as limit_usage_to_x_items,
    excluded_product_categories,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('coupons_ab1') }}
-- coupons
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

