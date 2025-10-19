import numpy as np
from sentence_transformers import SentenceTransformer
from typing import List, Dict, Any, Optional
import asyncio
import logging
from sqlalchemy import text
from database import get_db

logger = logging.getLogger(__name__)

class EmbeddingsService:
    def __init__(self):
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.embedding_dim = 384
        
    def generate_embedding(self, text: str) -> List[float]:
        try:
            if not text or text.strip() == "":
                return [0.0] * self.embedding_dim
            
            embedding = self.model.encode(text, convert_to_tensor=False)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return [0.0] * self.embedding_dim
    
    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        try:
            if not texts:
                return []
            
            valid_texts = [text for text in texts if text and text.strip()]
            if not valid_texts:
                return [[0.0] * self.embedding_dim] * len(texts)
            
            embeddings = self.model.encode(valid_texts, convert_to_tensor=False)
            
            result = []
            text_idx = 0
            for text in texts:
                if text and text.strip():
                    result.append(embeddings[text_idx].tolist())
                    text_idx += 1
                else:
                    result.append([0.0] * self.embedding_dim)
            
            return result
        except Exception as e:
            logger.error(f"Error generating batch embeddings: {e}")
            return [[0.0] * self.embedding_dim] * len(texts)
    
    def cosine_similarity(self, embedding1: List[float], embedding2: List[float]) -> float:
        try:
            vec1 = np.array(embedding1)
            vec2 = np.array(embedding2)
            
            dot_product = np.dot(vec1, vec2)
            norm1 = np.linalg.norm(vec1)
            norm2 = np.linalg.norm(vec2)
            
            if norm1 == 0 or norm2 == 0:
                return 0.0
            
            return dot_product / (norm1 * norm2)
        except Exception as e:
            logger.error(f"Error calculating cosine similarity: {e}")
            return 0.0
    
    async def update_contractor_embeddings(self, contractor_id: int, bio_text: str = None, services_text: str = None):
        try:
            combined_text = ""
            if bio_text:
                combined_text += bio_text
            if services_text:
                combined_text += " " + services_text
            
            if not combined_text.strip():
                combined_text = "No description available"
            
            embedding = self.generate_embedding(combined_text)
            
            async for db in get_db():
                await self._ensure_embeddings_table(db)
                
                upsert_sql = """
                INSERT INTO contractor_embeddings (contractor_id, embedding_text, embedding_vector, created_at, updated_at)
                VALUES (:contractor_id, :embedding_text, :embedding_vector, NOW(), NOW())
                ON CONFLICT (contractor_id) 
                DO UPDATE SET 
                    embedding_text = EXCLUDED.embedding_text,
                    embedding_vector = EXCLUDED.embedding_vector,
                    updated_at = NOW()
                """
                
                await db.execute(text(upsert_sql), {
                    "contractor_id": contractor_id,
                    "embedding_text": combined_text,
                    "embedding_vector": embedding
                })
                await db.commit()
                
                logger.info(f"Updated embeddings for contractor {contractor_id}")
                
        except Exception as e:
            logger.error(f"Error updating contractor embeddings: {e}")
            raise
    
    async def _ensure_embeddings_table(self, db):
        try:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS contractor_embeddings (
                id SERIAL PRIMARY KEY,
                contractor_id INTEGER UNIQUE NOT NULL,
                embedding_text TEXT,
                embedding_vector VECTOR(384),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (contractor_id) REFERENCES contractor(id) ON DELETE CASCADE
            );
            """
            await db.execute(text(create_table_sql))
            await db.commit()
        except Exception as e:
            logger.error(f"Error creating embeddings table: {e}")
            raise
    
    async def search_by_similarity(self, query: str, limit: int = 10, threshold: float = 0.3) -> List[Dict[str, Any]]:
        try:
            query_embedding = self.generate_embedding(query)
            
            async for db in get_db():
                await self._ensure_embeddings_table(db)
                
                search_sql = """
                SELECT 
                    c.id, c.name, c.phone, c.email, c.city, c.province,
                    c.bio_text, c.services_text, c.has_license, c.has_insurance,
                    c.hourly_rate_min, c.hourly_rate_max, c.created_at,
                    ce.embedding_text,
                    1 - (ce.embedding_vector <=> :query_embedding) as similarity_score
                FROM contractor c
                LEFT JOIN contractor_embeddings ce ON c.id = ce.contractor_id
                WHERE ce.embedding_vector IS NOT NULL
                ORDER BY ce.embedding_vector <=> :query_embedding
                LIMIT :limit
                """
                
                result = await db.execute(text(search_sql), {
                    "query_embedding": query_embedding,
                    "limit": limit
                })
                
                contractors = result.fetchall()
                
                results = []
                for c in contractors:
                    similarity_score = float(c[14]) if c[14] is not None else 0.0
                    
                    if similarity_score >= threshold:
                        contractor_data = {
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
                            "similarity_score": similarity_score,
                            "embedding_text": c[13]
                        }
                        results.append(contractor_data)
                
                return results
                
        except Exception as e:
            logger.error(f"Error in similarity search: {e}")
            return []
    
    async def update_all_embeddings(self):
        try:
            async for db in get_db():
                result = await db.execute(text("""
                    SELECT id, bio_text, services_text 
                    FROM contractor 
                    WHERE bio_text IS NOT NULL OR services_text IS NOT NULL
                """))
                
                contractors = result.fetchall()
                
                for contractor in contractors:
                    contractor_id = contractor[0]
                    bio_text = contractor[1]
                    services_text = contractor[2]
                    
                    await self.update_contractor_embeddings(contractor_id, bio_text, services_text)
                
                logger.info(f"Updated embeddings for {len(contractors)} contractors")
                
        except Exception as e:
            logger.error(f"Error updating all embeddings: {e}")
            raise
