from cachetools import TTLCache
from .config import settings

# Long TTL: ticker / owner / S&P 500 constituent lists
item_cache = TTLCache(maxsize=100, ttl=settings.CACHE_TTL_SECONDS)

# Short TTL: dashboard queries that scan fact tables
summary_cache = TTLCache(maxsize=64, ttl=settings.CACHE_SUMMARY_TTL_SECONDS)
transactions_cache = TTLCache(maxsize=256, ttl=settings.CACHE_TRANSACTIONS_TTL_SECONDS)
clusters_cache = TTLCache(maxsize=64, ttl=settings.CACHE_CLUSTERS_TTL_SECONDS)
cluster_breakdown_cache = TTLCache(
    maxsize=256, ttl=settings.CACHE_CLUSTER_BREAKDOWN_TTL_SECONDS
)


def get_cached_item(key: str):
    return item_cache.get(key)


def set_cached_item(key: str, value):
    item_cache[key] = value


def get_summary_cache(key: str):
    return summary_cache.get(key)


def set_summary_cache(key: str, value):
    summary_cache[key] = value


def get_transactions_cache(key: str):
    return transactions_cache.get(key)


def set_transactions_cache(key: str, value):
    transactions_cache[key] = value


def get_clusters_cache(key: str):
    return clusters_cache.get(key)


def set_clusters_cache(key: str, value):
    clusters_cache[key] = value


def get_cluster_breakdown_cache(key: str):
    return cluster_breakdown_cache.get(key)


def set_cluster_breakdown_cache(key: str, value):
    cluster_breakdown_cache[key] = value
