{{ config(
    indexes = [{'columns':['_airbyte_unique_key'],'unique':True}],
    unique_key = "_airbyte_unique_key",
    schema = "public",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('products_scd') }}
select
    _airbyte_unique_key,
    {{ adapter.quote('id') }} ,
    title,
    created_at as date_created,
    updated_at as date_modified,
    product_type,
    status,
    body_html as descriptions,
    tags,
    image,
    images,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_products_hashid
from {{ ref('products_scd') }}
-- products from {{ source('public', '_airbyte_raw_products') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

