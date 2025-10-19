from typing import Dict, Any, List, Optional
from sqlalchemy import text
from database import get_db
# from database import ContractorDB  
# from config import settings  
from rag_service import RAGService
from embeddings_service import EmbeddingsService
from cache_service import CacheService
import logging

logger = logging.getLogger(__name__)

class SearchService:
    def __init__(self):
        self.rag = RAGService()
        self.embeddings = EmbeddingsService()
        self.cache = CacheService()
    
    async def search(self, params):
        try:
            query = params.get("query", "")
            
            # Check cache first
            cached_result = await self.cache.get_cached_search_result(query)
            if cached_result:
                logger.info(f"Returning cached search result for query: {query}")
                return cached_result
            
            async for db in get_db():
                sql = "SELECT id, name, phone, email, city, province, bio_text, services_text, has_license, has_insurance, hourly_rate_min, hourly_rate_max, created_at FROM contractor"
                
                result = await db.execute(text(sql))
                contractors = result.fetchall()
                
                results = []
                for c in contractors:
                    data = {
                        "id": str(c[0]),
                        "name": c[1],
                        "phone": c[2],
                        "email": c[3],
                        "city": c[4],
                        "province": c[5],
                        "bio_text": c[6],
                        "services_text": c[7],
                        "has_license": c[8],
                        "has_insurance": c[9],
                        "hourly_rate_min": c[10],
                        "hourly_rate_max": c[11],
                        "created_at": c[12].isoformat() if c[12] else None,
                        "updated_at": c[12].isoformat() if c[12] else None
                    }
                    results.append(data)
                
                search_result = {
                    "contractors": results,
                    "total_count": len(results),
                    "query": query
                }
                
                # Cache the result
                await self.cache.cache_search_result(query, search_result)
                
                return search_result
                
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise
    
    async def rag_search(self, params):
        try:
            query = params.get("query", "")
            
            # Check cache first
            cached_rag_result = await self.cache.get_cached_rag_result(query)
            if cached_rag_result:
                logger.info(f"Returning cached RAG result for query: {query}")
                return {
                    "answer": cached_rag_result["rag_result"]["answer"],
                    "key_insights": cached_rag_result["rag_result"]["key_insights"],
                    "contractors": cached_rag_result["contractors"],
                    "total_count": len(cached_rag_result["contractors"]),
                    "query": query,
                    "sources": cached_rag_result["rag_result"]["sources"],
                    "generated_at": cached_rag_result["rag_result"].get("generated_at"),
                    "cached": True
                }
            
            # Try semantic search first
            semantic_results = await self.semantic_search(params)
            if semantic_results:
                contractors = semantic_results
            else:
                # Fallback to regular search
                search_results = await self.search(params)
                contractors = search_results.get("contractors", [])
            
            rag_result = await self.rag.generate_answer(
                query=query,
                contractors=contractors
            )
            
            final_result = {
                "answer": rag_result["answer"],
                "key_insights": rag_result["key_insights"],
                "contractors": contractors,
                "total_count": len(contractors),
                "query": query,
                "sources": rag_result["sources"],
                "generated_at": rag_result.get("generated_at")
            }
            
            # Cache the RAG result
            await self.cache.cache_rag_result(query, contractors, rag_result)
            
            return final_result
            
        except Exception as e:
            logger.error(f"RAG search failed: {e}")
            return await self.search(params)
    
    async def semantic_search(self, params):
        try:
            query = params.get("query", "")
            if not query:
                return []
            
            results = await self.embeddings.search_by_similarity(query, limit=20, threshold=0.3)
            return results
            
        except Exception as e:
            logger.error(f"Semantic search failed: {e}")
            return []
    
    async def update_contractor_embeddings(self, contractor_id: int):
        """Update embeddings for a specific contractor"""
        try:
            async for db in get_db():
                result = await db.execute(text("""
                    SELECT bio_text, services_text 
                    FROM contractor 
                    WHERE id = :contractor_id
                """), {"contractor_id": contractor_id})
                
                contractor = result.fetchone()
                if contractor:
                    bio_text = contractor[0]
                    services_text = contractor[1]
                    
                    await self.embeddings.update_contractor_embeddings(
                        contractor_id, bio_text, services_text
                    )
                    
                    # Invalidate cache for this contractor
                    await self.cache.invalidate_contractor_cache(contractor_id)
                    
                    logger.info(f"Updated embeddings for contractor {contractor_id}")
                    
        except Exception as e:
            logger.error(f"Error updating contractor embeddings: {e}")
            raise
    