import os
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv
import asyncio
import aiohttp
import schedule
import threading
import time
load_dotenv()
app = FastAPI()

API_KEY = os.getenv("PIPEDRIVE_API_KEY_ORG")
BASE_URL = 'https://api.pipedrive.com/v1'

if not API_KEY:
    raise ValueError("API key not found. Please set the PIPEDRIVE_API_KEY environment variable.")

class Organization(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    pop: Optional[int] = None
    pohd: Optional[int] = None
    ftve: Optional[int] = None

class OrganizationRelationship(BaseModel):
    related_org_id: int
    related_org_name: str

class OrganizationWithRelationships(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    pop: Optional[int] = None
    pohd: Optional[int] = None
    ftve: Optional[int] = None
    related_organizations: List[Organization] = []
    total_pop: int = 0
    total_pohd: int = 0
    total_ftve: int = 0


request_counter = 0
counter_lock = asyncio.Lock()

# @app.get("/organizations")
async def get_organizations(start: int = 0, limit: int = 100):
    global request_counter
    async with aiohttp.ClientSession() as session:
        all_related_org_ids_set = set()
        pagination = {'start': start, 'limit': limit, 'more_items_in_collection': True}
        updated_orgs = []

        while True:
            organizations = []
            fetched_data, pag_info = await fetch_organizations(session, pagination['start'], pagination['limit'])
            organizations.extend(fetched_data)
            
            pag_info['start'] += pag_info['limit']
            pagination.update(pag_info)
           
            org_relationships = {}
            
            for org in organizations:
                if org.id not in all_related_org_ids_set:
                    relationships = await get_organization_relationships(session, org.id)
                    org_relationships[org.id] = relationships
                    related_org_ids = [rel.related_org_id for rel in relationships]
                    all_related_org_ids_set.update(related_org_ids)
                    
                    related_orgs = await get_organizations_data(session, related_org_ids)
                    total_pop, total_pohd, total_ftve = 0, 0, 0
                    for related_org in related_orgs:
                        if related_org:
                            total_pop += related_org.pop or 0
                            total_pohd += related_org.pohd or 0
                            total_ftve += related_org.ftve or 0
                    
                    for related_org in related_orgs:
                        res = await update_organization_totals(session, related_org.id, total_pop, total_pohd, total_ftve)
                        updated_orgs.append(res)
                        
                        async with counter_lock:
                            request_counter += 1
                            if request_counter % 8 == 0:
                                await asyncio.sleep(2)
                  
            if not pag_info.get('more_items_in_collection', True):
                break

            print(f"Fetched {len(organizations)} organizations.")
            print(f"Pagination Info: {pagination}")
            
        return updated_orgs

async def fetch_organizations(session: aiohttp.ClientSession, start: int, limit: int):
    global request_counter
    url = f'{BASE_URL}/organizations'
    params = {
        'api_token': API_KEY,
        'start': start,
        'limit': limit
    }
    
    async with session.get(url, params=params) as response:
        if response.status != 200:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"API request failed with status code {response.status}"
            )
        result = await response.json()
    
    if 'data' not in result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected response format"
        )
    
    organizations = result['data']
    pagination = result.get('additional_data', {}).get('pagination', {})
    
    async with counter_lock:
        request_counter += 1
        if request_counter % 8 == 0:
            await asyncio.sleep(2)
    
    return (
        [
            Organization(
                id=org['id'],
                name=org['name'],
                address=org.get('address'),
                pop=int(org.get('ff657a191c25a9f57f6ee2186961198be9a77aaa') or 0),
                pohd=int(org.get('d54693358b3240110f7a963b45a9226bb3e41e30') or 0),
                ftve=int(org.get('5466322ffc600fa0d94bc88bb0a361de57035546') or 0)
            )
            for org in organizations
        ],
        {
            'start': pagination.get('start', start),
            'limit': pagination.get('limit', limit),
            'more_items_in_collection': pagination.get('more_items_in_collection', True)
        }
    )

async def get_organization_relationships(session: aiohttp.ClientSession, org_id: int) -> List[OrganizationRelationship]:
    global request_counter
    url = f'{BASE_URL}/organizationRelationships?org_id={org_id}'
    params = {
        'api_token': API_KEY
    }
    
    async with session.get(url, params=params) as response:
        if response.status == 404:
            return []
        
        if response.status != 200:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"API request failed with status code {response.status}: {response.text}"
            )
        
        result = await response.json()
        
        if 'related_objects' not in result or result['related_objects'] is None:
            return []
        
        related_objects = result['related_objects'].get('organization', {})
        
        async with counter_lock:
            request_counter += 1
            if request_counter % 8 == 0:
                await asyncio.sleep(2)
        
        return [
            OrganizationRelationship(
                related_org_id=int(org_id),
                related_org_name=org_details.get('name', '')
            )
            for org_id, org_details in related_objects.items()
        ]

async def get_organizations_data(session: aiohttp.ClientSession, org_ids: List[int]) -> List[Organization]:
    tasks = []
    for org_id in org_ids:
        url = f'{BASE_URL}/organizations/{org_id}'
        params = {
            'api_token': API_KEY
        }
        tasks.append(fetch_organization(session, url, params))
    
    responses = await asyncio.gather(*tasks)
    return responses

async def fetch_organization(session: aiohttp.ClientSession, url: str, params: dict) -> Organization:
    global request_counter
    async with session.get(url, params=params) as response:
        if response.status != 200:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch organization from URL: {url}"
            )
        org = await response.json()
        org_data = org.get('data')
        if not org_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Organization not found"
            )
        return Organization(
            id=org_data['id'],
            name=org_data['name'],
            address=org_data.get('address'),
            pop=int(org_data.get('ff657a191c25a9f57f6ee2186961198be9a77aaa') or 0),
            pohd=int(org_data.get('d54693358b3240110f7a963b45a9226bb3e41e30') or 0),
            ftve=int(org_data.get('5466322ffc600fa0d94bc88bb0a361de57035546') or 0)
        )

async def update_organization_totals(session: aiohttp.ClientSession, org_id: int, total_pop: int, total_pohd: int, total_ftve: int):
    global request_counter
    url = f'{BASE_URL}/organizations/{org_id}'
    params = {
        'api_token': API_KEY
    }
    data = {
        'f694db99056280fa7287ec6e969531925d9281e0': total_pop,
        '127511af7a0623e3b19f9ae5be3ae9d8f7651dca': total_pohd,
        '70db755cdb252a09b19905a85e51d98523d96673': total_ftve
    }
    
    async with session.put(url, params=params, json=data) as response:
        if response.status != 200:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update organization {org_id} with status code {response.status}"
            )
        async with counter_lock:
            request_counter += 1
            if request_counter % 8 == 0:
                await asyncio.sleep(2)
        return await response.json()
    
def job():
    print("job started")
    asyncio.run(get_organizations(start=0, limit=100))


schedule.every().day.at("00:01").do(job)

def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(60)  
if __name__ == "__main__":
    
    threading.Thread(target=run_scheduler, daemon=True).start()
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)