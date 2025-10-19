from sqlalchemy import Column, String, Float, Boolean, DateTime, Text, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from pgvector.sqlalchemy import Vector
from datetime import datetime
import uuid
from typing import AsyncGenerator

from config import settings

engine = create_async_engine(
    settings.database_url.replace("postgresql://", "postgresql+asyncpg://"),
    echo=False
)

# AsyncSessionLocal = async_sessionmaker(engine, class_=async_sessionmaker, expire_on_commit=True)
AsyncSessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

class ContractorDB(Base):
    __tablename__ = "contractor"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    phone = Column(String(50))
    email = Column(String(255))
    website = Column(String(500))
    address = Column(Text)
    city = Column(String(100))
    province = Column(String(50))
    postal = Column(String(20))
    country = Column(String(50), default="CA")
    bio_text = Column(Text)
    services_text = Column(Text)
    review_text = Column(Text)
    has_license = Column(Boolean, default=False)
    has_insurance = Column(Boolean, default=False)
    hourly_rate_min = Column(Float)
    hourly_rate_max = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    embedding = Column(Vector(384))

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

async def init_db():
    try:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS unaccent"))
            
            try:
                await conn.run_sync(Base.metadata.create_all)
            except Exception as e:
                if "already exists" in str(e):
                    print("tables already exist")
                else:
                    raise
        
        print("database ready")
        
    except Exception as e:
        print(f"db init failed: {e}")
        raise