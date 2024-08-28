import json
from typing import List, Dict
from pipedriverapi import PersonCreate, OrganizationCreate,LeadCreate, create_or_get_person, create_or_get_organization, create_lead

def read_projects_from_file(file_path: str) -> List[Dict]:
    """Reads a file containing multiple JSON objects separated by a delimiter and returns a list of dictionaries."""
    projects_list = []
    delimiter = '----------------------------------------'  # Define your delimiter
    
    with open(file_path, 'r') as file:
        content = file.read()
    
    # Split the content based on the delimiter
    entries = content.split(delimiter)
    
    for entry in entries:
        entry = entry.strip()  # Remove any extra whitespace or newlines
        if entry:  # Ensure the entry is not empty
            try:
                project = json.loads(entry)
                projects_list.append(project)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for entry: {entry}\nError: {e}")
    return projects_list

def process_projects(projects_list: List[Dict]) -> Dict[str, Dict[str, str]]:
    collected_ids = {
        "persons": {},
        "organizations": {}
    }
    
    for project in projects_list[:10]:  # Process the first 10 projects        
        person_id = None
        org_id = None
        
        keycard_contact = project.get("KeyCard Full Contact")
        if keycard_contact and keycard_contact.strip():
            person_data = PersonCreate(
                name=keycard_contact,
                email=project.get("Email"),
                phone=project.get("Phone")
            )
            
            try:
                person_response = create_or_get_person(person_data)
                person_id = person_response.get("id")
                if person_id:
                    collected_ids["persons"][keycard_contact] = person_id
                print(f"Person Response: {person_response}")
            except Exception as e:
                print(f"An error occurred while creating or getting person: {e}")
        
        office_names = project.get("Office Names")
        if office_names and office_names.strip():
            org_data = OrganizationCreate(
                name=office_names,
            )
            
            try:
                org_response = create_or_get_organization(org_data)
                org_id = org_response.get("id")
                if org_id:
                    collected_ids["organizations"][office_names] = org_id
                print(f"Organization Response: {org_response}")
            except Exception as e:
                print(f"An error occurred while creating or getting organization: {e}")
        
        if person_id or org_id:
            lead_data = {
                "title": project.get("Site Name"),
                "organization_id": org_id,
                "person_id": person_id,
                "e6627de564fe2d29bd1af0df8a5bc9a8e5f449d8": project.get("Contract Stage"),
                "b75e66677deecfea4523a24f5bcb905da8af326d": project.get("Project Postcode"),
                "d9b808b6d14ec2817dc71254f4460fd87ccf48e2": "www.glenigans.co.uk/testentry",
                "88a57733d6e78dbc8ca33f5d2bb636c712e49dac": project.get("Start Date"),
                "49838a60ad9bb658a485663cb3c57516d7fb38f1": project.get("Sectors"),
                "46844d5679053d9b4cb44ed674ada67eddd9f7b8": project.get("Role Locations Level 2")
            }
            # print(lead_data.dict())  # Optional: Print the dictionary representation
            
            try:
                create_lead_response = create_lead(lead_data)
                print(f"Lead Response: {create_lead_response}")
            except Exception as e:
                print(f"An error occurred while creating lead: {e}")
    
    return collected_ids

if __name__ == "__main__":
    file_path = 'projects_data.txt'  # Path to your file
    projects_list = read_projects_from_file(file_path)
    process_projects(projects_list)
