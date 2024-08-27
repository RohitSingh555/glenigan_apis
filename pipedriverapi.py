import requests
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

app = FastAPI()

API_KEY = ''
BASE_URL = 'https://api.pipedrive.com/v1'

if not API_KEY:
    raise ValueError("API key not found. Please set the PIPEDRIVE_API_KEY environment variable.")

class PersonCreate(BaseModel):
    name: str
    email: str = None
    phone: str = None

class OrganizationCreate(BaseModel):
    name: str
    address: str = None
    phone: str = None

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

@app.post('/persons')
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

@app.post('/organizations')
def create_or_get_organization(organization: OrganizationCreate):
    # Check if the organization already exists
    organization_id = check_organization_exists(organization.name)
    if organization_id:
        return {"id": organization_id}
    
    # Create a new organization
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

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
