import os
import requests
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional, List
from dotenv import load_dotenv
import asyncio
import aiohttp

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
    pohu: Optional[int] = None
    ftve: Optional[int] = None

class OrganizationRelationship(BaseModel):
    related_org_id: int
    related_org_name: str

class OrganizationWithRelationships(BaseModel):
    id: int
    name: str
    address: Optional[str] = None
    pop: Optional[int] = None
    pohu: Optional[int] = None
    ftve: Optional[int] = None
    related_organizations: List[Organization] = []
    total_pop: int = 0
    total_pohu: int = 0
    total_ftve: int = 0

@app.get("/organizations", response_model=List[OrganizationWithRelationships])
async def get_organizations(start: int = 0, limit: int = 10):
    async with aiohttp.ClientSession() as session:
        organizations = await fetch_organizations(session, start, limit)
        
        all_related_org_ids = set()
        org_relationships = {}
        
        for org in organizations:
            relationships = await get_organization_relationships(session, org.id)
            org_relationships[org.id] = relationships
            related_org_ids = {rel.related_org_id for rel in relationships}
            all_related_org_ids.update(related_org_ids)
        
        related_orgs = await get_organizations_data(session, all_related_org_ids)
        related_org_map = {org.id: org for org in related_orgs}
        
        orgs_with_relationships = []
        
        for org in organizations:
            relationships = org_relationships.get(org.id, [])
            related_orgs_details = []
            total_pop, total_pohu, total_ftve = 0, 0, 0
            
            for rel in relationships:
                related_org = related_org_map.get(rel.related_org_id)
                if related_org:
                    related_orgs_details.append(related_org)
                    total_pop += related_org.pop or 0
                    total_pohu += related_org.pohu or 0
                    total_ftve += related_org.ftve or 0
            
            if org not in related_orgs_details:
                related_orgs_details.append(org)

            orgs_with_relationships.append(
                OrganizationWithRelationships(
                    id=org.id,
                    name=org.name,
                    address=org.address,
                    pop=org.pop,
                    pohu=org.pohu,
                    ftve=org.ftve,
                    related_organizations=related_orgs_details,
                    total_pop=total_pop,
                    total_pohu=total_pohu,
                    total_ftve=total_ftve
                )
            )
            
            await update_organization_totals(session, org.id, total_pop, total_pohu, total_ftve)
        
        return orgs_with_relationships

async def fetch_organizations(session: aiohttp.ClientSession, start: int, limit: int) -> List[Organization]:
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
    
    return [
        Organization(
            id=org['id'],
            name=org['name'],
            address=org.get('address'),
            pop=org.get('ff657a191c25a9f57f6ee2186961198be9a77aaa'),
            pohu=org.get('d54693358b3240110f7a963b45a9226bb3e41e30'),
            ftve=org.get('5466322ffc600fa0d94bc88bb0a361de57035546')
        )
        for org in organizations
    ]

async def get_organization_relationships(session: aiohttp.ClientSession, org_id: int) -> List[OrganizationRelationship]:
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
        
        related_objects = result['related_objects']['organization']
        
        return [
            OrganizationRelationship(
                related_org_id=int(org_id),
                related_org_name=org_details.get('name', '')
            )
            for org_id, org_details in related_objects.items()
        ]

async def get_organizations_data(session: aiohttp.ClientSession, org_ids: set) -> List[Organization]:
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
            pop=org_data.get('ff657a191c25a9f57f6ee2186961198be9a77aaa'),
            pohu=org_data.get('d54693358b3240110f7a963b45a9226bb3e41e30'),
            ftve=org_data.get('5466322ffc600fa0d94bc88bb0a361de57035546')
        )

async def update_organization_totals(session: aiohttp.ClientSession, org_id: int, total_pop: int, total_pohu: int, total_ftve: int):
    url = f'{BASE_URL}/organizations/{org_id}'
    params = {
        'api_token': API_KEY
    }
    data = {
        'f694db99056280fa7287ec6e969531925d9281e0': total_pop,
        '127511af7a0623e3b19f9ae5be3ae9d8f7651dca': total_pohu,
        '70db755cdb252a09b19905a85e51d98523d96673': total_ftve
    }
    
    async with session.put(url, params=params, json=data) as response:
        if response.status != 200:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to update organization {org_id} with status code {response.status}"
            )
        return await response.json()

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
