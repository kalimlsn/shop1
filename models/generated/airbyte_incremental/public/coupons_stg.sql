{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('coupons_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        'code',
        '_links',
        'amount',
        array_to_string('used_by'),
        'shop_url',
        'meta_data',
        'description',
        array_to_string('product_ids'),
        'usage_count',
        'usage_limit',
        'date_created',
        'date_expires',
        'date_modified',
        'discount_type',
        boolean_to_string('free_shipping'),
        boolean_to_string('individual_use'),
        'maximum_amount',
        'minimum_amount',
        'date_created_gmt',
        'date_expires_gmt',
        'date_modified_gmt',
        array_to_string('email_restrictions'),
        boolean_to_string('exclude_sale_items'),
        array_to_string('product_categories'),
        array_to_string('excluded_product_ids'),
        'usage_limit_per_user',
        'limit_usage_to_x_items',
        array_to_string('excluded_product_categories'),
    ]) }} as _airbyte_coupons_hashid,
    tmp.*
from {{ ref('coupons_ab2') }} tmp
-- coupons
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}
