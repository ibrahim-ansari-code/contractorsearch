import json
import asyncio
import logging
from typing import Any, Optional, Dict, List
import redis.asyncio as redis
from datetime import datetime, timedelta
import hashlib

logger = logging.getLogger(__name__)

class CacheService:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client = None
        self.default_ttl = 3600  # 1 hour default TTL
        
    async def connect(self):
        try:
            self.redis_client = redis.from_url(self.redis_url)
            await self.redis_client.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None
    
    async def disconnect(self):
        if self.redis_client:
            await self.redis_client.close()
            self.redis_client = None
    
    def _generate_key(self, prefix: str, *args) -> str:
        key_parts = [prefix] + [str(arg) for arg in args]
        key_string = ":".join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    async def get(self, key: str) -> Optional[Any]:
        if not self.redis_client:
            return None
        
        try:
            value = await self.redis_client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Error getting from cache: {e}")
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        if not self.redis_client:
            return False
        
        try:
            ttl = ttl or self.default_ttl
            serialized_value = json.dumps(value, default=str)
            await self.redis_client.setex(key, ttl, serialized_value)
            return True
        except Exception as e:
            logger.error(f"Error setting cache: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        if not self.redis_client:
            return False
        
        try:
            await self.redis_client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Error deleting from cache: {e}")
            return False
    
    async def delete_pattern(self, pattern: str) -> int:
        if not self.redis_client:
            return 0
        
        try:
            keys = await self.redis_client.keys(pattern)
            if keys:
                return await self.redis_client.delete(*keys)
            return 0
        except Exception as e:
            logger.error(f"Error deleting pattern from cache: {e}")
            return 0
    
    # Search-specific caching methods
    async def cache_search_result(self, query: str, results: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        key = self._generate_key("search", query)
        return await self.set(key, results, ttl)
    
    async def get_cached_search_result(self, query: str) -> Optional[Dict[str, Any]]:
        key = self._generate_key("search", query)
        return await self.get(key)
    
    async def cache_embedding(self, contractor_id: int, embedding: List[float], ttl: Optional[int] = None) -> bool:
        key = self._generate_key("embedding", contractor_id)
        return await self.set(key, embedding, ttl)
    
    async def get_cached_embedding(self, contractor_id: int) -> Optional[List[float]]:
        key = self._generate_key("embedding", contractor_id)
        return await self.get(key)
    
    async def cache_rag_result(self, query: str, contractors: List[Dict], rag_result: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        key = self._generate_key("rag", query)
        cache_data = {
            "contractors": contractors,
            "rag_result": rag_result,
            "cached_at": datetime.utcnow().isoformat()
        }
        return await self.set(key, cache_data, ttl)
    
    async def get_cached_rag_result(self, query: str) -> Optional[Dict[str, Any]]:
        key = self._generate_key("rag", query)
        return await self.get(key)
    
    async def cache_contractor_data(self, contractor_id: int, contractor_data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        key = self._generate_key("contractor", contractor_id)
        return await self.set(key, contractor_data, ttl)
    
    async def get_cached_contractor_data(self, contractor_id: int) -> Optional[Dict[str, Any]]:
        key = self._generate_key("contractor", contractor_id)
        return await self.get(key)
    
    async def invalidate_contractor_cache(self, contractor_id: int) -> bool:
        try:
            await self.delete_pattern(f"*contractor:{contractor_id}*")
            await self.delete_pattern(f"*embedding:{contractor_id}*")
            return True
        except Exception as e:
            logger.error(f"Error invalidating contractor cache: {e}")
            return False
    
    async def invalidate_search_cache(self) -> bool:
        try:
            await self.delete_pattern("*search:*")
            await self.delete_pattern("*rag:*")
            return True
        except Exception as e:
            logger.error(f"Error invalidating search cache: {e}")
            return False
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        if not self.redis_client:
            return {"status": "disconnected"}
        
        try:
            info = await self.redis_client.info()
            return {
                "status": "connected",
                "used_memory": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0)
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"status": "error", "error": str(e)}
