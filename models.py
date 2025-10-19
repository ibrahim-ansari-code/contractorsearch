from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from uuid import UUID

class ContractorBase(BaseModel):
    name: str
    website: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    province: Optional[str] = None
    postal: Optional[str] = None
    country: Optional[str] = "CA"
    bio_text: Optional[str] = None
    services_text: Optional[str] = None
    has_license: Optional[bool] = False
    has_insurance: Optional[bool] = False
    hourly_rate_min: Optional[float] = None
    hourly_rate_max: Optional[float] = None

class ContractorCreate(ContractorBase):
    pass

class Contractor(ContractorBase):
    id: UUID
    created_at: datetime
    updated_at: datetime
