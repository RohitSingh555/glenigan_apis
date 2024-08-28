import requests

PIPEDRIVE_API_URL = "https://api.pipedrive.com/v1/"
PIPEDRIVE_API_TOKEN = "adc7bdaca33ac510f415fc01fa0a4aaaddb808c"

def create_lead(person_id: str, org_id: str) -> Dict[str, str]:
    url = f"{PIPEDRIVE_API_URL}deals"
    headers = {"Content-Type": "application/json"}
    data = {
        "title": "New Lead",
        "person_id": person_id,
        "org_id": org_id,
        "api_token": PIPEDRIVE_API_TOKEN
    }
    response = requests.post(url, json=data, headers=headers)
    return response.json()
