from typing import Dict, Any
from sqlalchemy import text
from database import get_db
# from database import ContractorDB  
# from config import settings  
from rag_service import RAGService

class SearchService:
    def __init__(self):
        self.rag = RAGService()
    
    async def search(self, params):
        try:
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
                
                return {
                    "contractors": results,
                    "total_count": len(results),
                    "query": params.get("query", "")
                }
                
        except Exception as e:
            print(f"Search failed: {e}")
            raise
    
    async def rag_search(self, params):
        try:
            search_results = await self.search(params)
            contractors = search_results.get("contractors", [])
            
            rag_result = await self.rag.generate_answer(
                query=params.get("query", ""),
                contractors=contractors
            )
            
            return {
                "answer": rag_result["answer"],
                "key_insights": rag_result["key_insights"],
                "contractors": contractors,
                "total_count": len(contractors),
                "query": params.get("query", ""),
                "sources": rag_result["sources"],
                "generated_at": rag_result.get("generated_at")
            }
            
        except Exception as e:
            print(f"fail {e}")
            return await self.search(params)
    