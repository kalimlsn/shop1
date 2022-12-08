{{ config(
    alias='spf_products_stg',
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('products_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        object_to_string('tags'),
        object_to_string('image'),
        'title',
        'handle',
        array_to_string('images'),
        'status',
        'vendor',
        array_to_string(adapter.quote('options')),
        'shop_url',
        array_to_string('variants'),
        'body_html',
        'created_at',
        'updated_at',
        'product_type',
        'published_at',
        'published_scope',
        'template_suffix',
        'admin_graphql_api_id',
    ]) }} as _airbyte_products_hashid,
    tmp.*
from {{ ref('products_ab2') }} tmp
-- products
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}

