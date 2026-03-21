from cachetools import TTLCache
from .config import settings

# In-memory cache for company and owner lists
item_cache = TTLCache(maxsize=100, ttl=settings.CACHE_TTL_SECONDS)

def get_cached_item(key: str):
    return item_cache.get(key)

def set_cached_item(key: str, value):
    item_cache[key] = value
