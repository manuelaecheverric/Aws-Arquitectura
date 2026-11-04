select
    cast(review_id as bigint) as review_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(rating as integer) as rating,
    cast(reviewed_at as timestamptz) as reviewed_at,
    {{ text_to_boolean("json_extract_path_text(document_json, 'verification', 'purchase_confirmed', true)") }} as is_verified_purchase,
    document_json,
    _batch_id,
    _loaded_at,
    _source
from {{ latest_version(source('mongodb_raw', 'mongo_reviews'), 'review_id') }}
