import requests
from fastapi import FastAPI, HTTPException, status, Query
from pydantic import BaseModel
from typing import Optional, Dict
app = FastAPI()

API_KEY = 'adc7bdaca33ac510f415fc01fa0a4aaaddb808ce'
BASE_URL = 'https://api.pipedrive.com/v1'

if not API_KEY:
    raise ValueError("API key not found. Please set the PIPEDRIVE_API_KEY environment variable.")

class PersonCreate(BaseModel):
    name: str
    email: str = None

class OrganizationCreate(BaseModel):
    name: str
    address: str = None
    phone: str = None
class LeadCreate(BaseModel):
    title: str
    person_id: int
    organization_id: int
    custom_fields: Optional[Dict[str, str]] = None 

def check_person_exists(name: str):
    url = f'{BASE_URL}/persons/search'
    params = {
        'api_token': API_KEY,
        'term': name,
        'fields': 'name'
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"API request failed with status code {response.status_code}"
        )
    
    data = response.json()
    
    if 'data' not in data:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected response format"
        )
    
    for person in data['data']['items']:
        if person['item']['name'] == name:
            return person['item']['id']
    
    return None

def check_organization_exists(name: str):
    url = f'{BASE_URL}/organizations/search'
    params = {
        'api_token': API_KEY,
        'term': name,
        'fields': 'name'
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"API request failed with status code {response.status_code}"
        )
    
    data = response.json()
    
    if 'data' not in data:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected response format"
        )
    
    for organization in data['data']['items']:
        if organization['item']['name'] == name:
            return organization['item']['id']
    
    return None

def create_or_get_person(person: PersonCreate):
    # Check if the person already exists
    person_id = check_person_exists(person.name)
    if person_id:
        return {"id": person_id}
    
    # Create a new person
    url = f'{BASE_URL}/persons'
    params = {'api_token': API_KEY}
    data = person.dict()
    
    response = requests.post(url, params=params, json=data)
    if response.status_code != 201:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"API request failed with status code {response.status_code}"
        )
    
    result = response.json()
    
    if 'data' not in result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected response format"
        )
    
    return {"id": result['data']['id']}

def create_or_get_organization(organization: OrganizationCreate):
    organization_id = check_organization_exists(organization.name)
    if organization_id:
        return {"id": organization_id}
    url = f'{BASE_URL}/organizations'
    params = {'api_token': API_KEY}
    data = organization.dict()
    
    response = requests.post(url, params=params, json=data)
    if response.status_code != 201:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"API request failed with status code {response.status_code}"
        )
    
    result = response.json()
    
    if 'data' not in result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected response format"
        )
    
    return {"id": result['data']['id']}



def check_lead_exists(email: str) -> str:

    url = f'{BASE_URL}/leads'
    params = {'api_token': API_KEY, 'email': email}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        result = response.json()
        if result.get('data'):
            return result['data']['id']  # Return the lead ID if found
    return None
@app.get('/leader')
def fetch_all_leads_from_api() -> dict:
    url = f'{BASE_URL}/leads'
    params = {
        'api_token': API_KEY,
        # 'sort_by': 'add_time', 
        'start': 201,
        
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPException(
            status_code=response.status_code,
            detail=f"API request failed with status code {response.status_code}"
        )
  
@app.post('/lead')
def create_lead(data: dict):
    params = {'api_token': API_KEY}
    url = f'{BASE_URL}/leads'
    print(f"URL: {url}")
    print(f"Request Data: {data}")

    try:
        response = requests.post(url, params=params, json=data)
        response.raise_for_status()  # Raise an exception for HTTP errors

        return response.json()
    
    except requests.RequestException as e:
        # Log the full response content if available
        error_detail = response.text if response else str(e)
        print(f"Request failed with status code {response.status_code if response else 'unknown'}")
        print(f"Response content: {error_detail}")

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"API request failed: {error_detail}"
        )
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)