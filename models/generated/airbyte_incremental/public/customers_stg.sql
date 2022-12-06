{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_public",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('customers_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        adapter.quote('id'),
        adapter.quote('role'),
        'email',
        '_links',
        'billing',
        'shipping',
        'shop_url',
        'username',
        'last_name',
        'meta_data',
        'avatar_url',
        'first_name',
        'date_created',
        'date_modified',
        'date_created_gmt',
        'date_modified_gmt',
        boolean_to_string('is_paying_customer'),
    ]) }} as _airbyte_customers_hashid,
    tmp.*
from {{ ref('customers_ab2') }} tmp
-- customers
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at', this) }}
