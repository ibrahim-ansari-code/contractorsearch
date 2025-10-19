from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging
from sqlalchemy import text
# from models import Contractor
from database import get_db, init_db
from search_service import SearchService
from ingest_service import IngestService
# from config import settings 

logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# search_service = SearchService() 
search_service = None
ingest_service = None

@app.on_event("startup")
async def startup_event():
    global search_service, ingest_service
    await init_db()
    search_service = SearchService()
    ingest_service = IngestService()
    print("db ready")

@app.get("/health")
async def health_check():
    try:
        async for db in get_db():
            result = await db.execute(text("SELECT COUNT(*) FROM contractor"))
            count = result.scalar()
            return {
                "status": "healthy",
                "contractors": count,
                "timestamp": datetime.utcnow().isoformat()
            }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@app.get("/search")
async def search_contractors(
    q: str = Query(..., description="Search query"),
    db=Depends(get_db)
):
    try:
        # search_params = {
        #     "query": q,
        #     "lat": lat,
        #     "lon": lon,
        #     "radius_km": radius_km,
        #     "trades": trade,
        #     "min_rating": min_rating,
        #     "licensed": licensed,
        #     "insured": insured,
        #     "max_rate": max_rate,
        #     "limit": limit,
        #     "offset": offset
        # }
        
        params = {
            "query": q
        }
        
        results = await search_service.rag_search(params)
        
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@app.post("/scrape")
async def scrape_url(url: str = Query(..., description="URL to scrape")):
    try:
        result = await ingest_service.scrape_url(url)
        return {
            "status": "success",
            "url": url,
            "data": result,
            "message": "Success"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scraping failed: {str(e)}")

@app.post("/scrape-and-save")
async def scrape_and_save(url: str = Query(..., description="URL to scrape and save")):
    try:
        scraped_data = await ingest_service.scrape_url(url)
        
        async for db in get_db():
            insert_sql = """
            INSERT INTO contractor (
                name, phone, email, website, city, province,
                bio_text, services_text, has_license, has_insurance,
                hourly_rate_min, hourly_rate_max, created_at, updated_at
            ) VALUES (
                :name, :phone, :email, :website, :city, :province,
                :bio_text, :services_text, :has_license, :has_insurance,
                :hourly_rate_min, :hourly_rate_max, NOW(), NOW()
            ) RETURNING id
            """
            
            result = await db.execute(text(insert_sql), {
                "name": scraped_data.get('name', 'Unknown'),
                "phone": scraped_data.get('phone'),
                "email": scraped_data.get('email'),
                "website": scraped_data.get('website', url),
                "city": scraped_data.get('city'),
                "province": scraped_data.get('province'),
                "bio_text": scraped_data.get('bio_text'),
                "services_text": scraped_data.get('services_text'),
                "has_license": scraped_data.get('has_license', False),
                "has_insurance": scraped_data.get('has_insurance', False),
                "hourly_rate_min": scraped_data.get('hourly_rate_min'),
                "hourly_rate_max": scraped_data.get('hourly_rate_max')
            })
            
            contractor_id = result.scalar()
            await db.commit()  # Commit the transaction
            print(f"Saved contractor with ID: {contractor_id}")
            
            return {
                "status": "success",
                "url": url,
                "contractor_id": str(contractor_id),
                "data": scraped_data,
                "message": "Successfully scraped and saved contractor data"
            }
        
    except Exception as e:
        print(f"Scraping and saving failed: {e}")
        raise HTTPException(status_code=500, detail=f"Scraping and saving failed: {str(e)}")

@app.get("/contractors/{contractor_id}")
async def get_contractor(contractor_id: str, db=Depends(get_db)):
    try:
        async for db in get_db():
            result = await db.execute("SELECT * FROM contractor WHERE id = :id", {"id": contractor_id})
            contractor = result.fetchone()
            if not contractor:
                raise HTTPException(status_code=404, detail="Contractor not found")
            return contractor
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# @app.get("/test")
# async def test_endpoint():
#     return {"message": "test"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)