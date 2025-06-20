# Performance Configuration for BigQuery Service
from typing import Dict, Any

# Cache configuration
CACHE_CONFIG = {
    "ttl_seconds": 300,  # 5 minutes default TTL
    "max_cache_size": 1000,  # Maximum number of cache entries
    "enable_caching": True,
}

# Query optimization settings
QUERY_CONFIG = {
    "max_bytes_billed": 1000000000,  # 1GB limit
    "use_query_cache": True,
    "use_legacy_sql": False,
    "max_results_per_query": 1000,  # Limit for unique values queries
    "enable_parallel_execution": True,
}

# Performance monitoring
PERFORMANCE_CONFIG = {
    "log_slow_queries": True,
    "slow_query_threshold_seconds": 5.0,
    "enable_query_metrics": True,
}

def get_performance_settings() -> Dict[str, Any]:
    """Get all performance settings"""
    return {
        "cache": CACHE_CONFIG,
        "query": QUERY_CONFIG,
        "monitoring": PERFORMANCE_CONFIG
    } 