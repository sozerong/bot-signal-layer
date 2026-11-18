select entity_type, entity_key, entity_name, truth_source, label, label_method, observed_at, known_at, raw_ref
from {{ source('silver', 'truth') }}
