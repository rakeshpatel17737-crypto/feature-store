"""Redis key patterns for the feature store."""

FEATURE_HASH_KEY = "features:{user_id}"
FEATURE_META_KEY = "features:{user_id}:meta"
BASELINE_DIST_KEY = "baseline:dist:{feature_name}"
USER_INDEX_KEY = "index:active_users"

FEATURE_FIELDS = [
    "txn_count_5m",
    "avg_spend_1h",
    "session_activity_rate",
    "cart_abandon_ratio",
    "product_interaction_freq",
    "anomaly_score",
    "computed_at",
]


def feature_key(user_id: str) -> str:
    return FEATURE_HASH_KEY.format(user_id=user_id)


def feature_meta_key(user_id: str) -> str:
    return FEATURE_META_KEY.format(user_id=user_id)


def baseline_key(feature_name: str) -> str:
    return BASELINE_DIST_KEY.format(feature_name=feature_name)
