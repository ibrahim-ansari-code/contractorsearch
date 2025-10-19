import asyncio
import re
from typing import Dict, Any, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import logging

from database import get_db, ContractorDB

logger = logging.getLogger(__name__)

class IngestService:
    def __init__(self):
        print("start scrape")

    async def scrape_url(self, url):
        print(f"URL: {url}")
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            html = response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"fail: {e}")
            raise

        soup = BeautifulSoup(html, 'html.parser')

        name = soup.find('h1') or soup.find('title')
        name = name.get_text(strip=True) if name else "Unknown"

        phone = None
        contractor_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(word in x.lower() for word in ['contractor', 'professional', 'profile', 'contact', 'info']))
        
        for section in contractor_sections:
            phone_tag = section.find('a', href=lambda x: x and x.startswith('tel:'))
            if phone_tag:
                phone = phone_tag.get('href').replace('tel:', '')
                break
        
        if not phone:
            phone_tag = soup.find('a', href=lambda x: x and x.startswith('tel:'))
            if phone_tag:
                phone = phone_tag.get('href').replace('tel:', '')

        email = None    
        email_tag = soup.find('a', href=lambda x: x and x.startswith('mailto:'))
        if email_tag:
            email = email_tag.get('href').replace('mailto:', '')

        bio_text = None
        bio_tag = soup.find('div', class_='bio') or soup.find('p')
        if bio_tag:
            bio_text = bio_tag.get_text(strip=True)[:500]

        services_text = None
        services_list = soup.find('ul', class_='services') or soup.find('div', class_='services')
        if services_list:
            services_text = ", ".join([li.get_text(strip=True) for li in services_list.find_all('li')])
        elif bio_text:
            services_text = bio_text[:500]

        city = None
        province = None
        
        content_text = (bio_text or "") + " " + html
        city_match = re.search(r'\b(Toronto|Hamilton|Mississauga|Brampton|Markham|Richmond Hill|Vaughan|Oakville|Burlington|Ajax|Kitchener|Waterloo|Guelph|London|Ottawa|Windsor|Barrie|Sudbury|Thunder Bay|Sault Ste Marie)\b', content_text, re.IGNORECASE) 
        if city_match:
            city = city_match.group(1)
            
        province_match = re.search(r'\b(Ontario|ON|Quebec|QC|British Columbia|BC|Alberta|AB|Manitoba|MB|Saskatchewan|SK|Nova Scotia|NS|New Brunswick|NB|Newfoundland|NL|Prince Edward Island|PEI|Northwest Territories|NT|Yukon|YT|Nunavut|NU)\b', content_text, re.IGNORECASE)
        if province_match:
            province = province_match.group(1)

        license_indicators = ['licensed', 'license', 'certified', 'certification', 'registered', 'bonded']
        insurance_indicators = ['insured', 'insurance', 'liability', 'coverage', 'bonded']
        
        content_lower = content_text.lower()
        has_license = any(indicator in content_lower for indicator in license_indicators)
        has_insurance = any(indicator in content_lower for indicator in insurance_indicators)
        
        hourly_rate_min = None
        hourly_rate_max = None

        return {
            "name": name,
            "phone": phone,
            "email": email,
            "website": url,
            "city": city,
            "province": province,
            "bio_text": bio_text,
            "services_text": services_text,
            "has_license": has_license,
            "has_insurance": has_insurance,
            "hourly_rate_min": hourly_rate_min,
            "hourly_rate_max": hourly_rate_max,
        }