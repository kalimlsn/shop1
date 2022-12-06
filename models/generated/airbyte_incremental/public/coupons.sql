{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('coupons_scd') }}
select
    _airbyte_unique_key,
    {{ adapter.quote('id') }},
    code,
    _links,
    amount,
    used_by,
    shop_url,
    meta_data,
    description,
    product_ids,
    usage_count,
    usage_limit,
    date_created,
    date_expires,
    date_modified,
    discount_type,
    free_shipping,
    individual_use,
    maximum_amount,
    minimum_amount,
    date_created_gmt,
    date_expires_gmt,
    date_modified_gmt,
    email_restrictions,
    exclude_sale_items,
    product_categories,
    excluded_product_ids,
    usage_limit_per_user,
    limit_usage_to_x_items,
    excluded_product_categories,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_coupons_hashid
from {{ ref('coupons_scd') }}
-- coupons from {{ source('public', '_airbyte_raw_coupons') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}
