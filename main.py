import requests
import math
import json
import os
from dotenv import load_dotenv
import schedule
import time
import datetime
from lead import read_projects_from_file, process_projects
load_dotenv()

def fetch_projects(api_key):
    url = f"https://www.gleniganapi.com/glenigan/project/_search?key={api_key}"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json"
    }
  
    payload_template = {
        "query": {
            "bool": {
                "filter": [
                    {
                        "terms": {
                            "ProjectSize": ["Mega", "Large"]
                        }
                    },
                    {
                        "range": {
                            "FirstPublished": {
                                "gte": "now-1y/y",
                                "lte": "now"
                            }
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "LatestEventDate": {
                    "order": "desc"
                }
            }
        ],
        "size": 50
    }

    projects_list = []  

    try:
        response = requests.post(url, headers=headers, json=payload_template)
        response.raise_for_status()
        data = response.json()
        
        total_projects = data.get('total', 0)
        pages_count = math.ceil(total_projects / 50)
        
        print(f"Total Projects Found: {total_projects}")
        print(f"Total Pages: {pages_count}")
        
        # Iterate through each page
        for page in range(pages_count):
            payload = payload_template.copy()
            payload["from"] = page * 50  
            
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            
            for project in results:
                project_source = project.get('source', {})
                
                office_names = project_source.get('OfficeNames', 'Details Not Available')
                
                roles_details = project_source.get('RolesDetails', [])
                if roles_details:
                    roles = roles_details[0].get('Roles', [])
                    if roles:
                        companies_in_role = roles[0].get('CompaniesInRole', [])
                        if companies_in_role:
                            contacts_in_company = companies_in_role[0].get('ContactsInCompanyInRole', [])
                            if contacts_in_company:
                                contact_info = contacts_in_company[0]
                                keycard_fullcontact = contact_info.get('KeyCard_FullContact', 'N/A')
                                job_title = contact_info.get('JobTitleForThisProject', 'N/A')
                                phone = contact_info.get('KeyCard_Phone1', 'N/A')
                            else:
                                keycard_fullcontact = 'N/A'
                                job_title = 'N/A'
                                phone = 'N/A'
                        else:
                            keycard_fullcontact = 'N/A'
                            job_title = 'N/A'
                            phone = 'N/A'
                    else:
                        keycard_fullcontact = 'N/A'
                        job_title = 'N/A'
                        phone = 'N/A'
                else:
                    keycard_fullcontact = 'N/A'
                    job_title = 'N/A'
                    phone = 'N/A'
                
                contract_stage = project_source.get('ContractStage', 'N/A')
                site_name = project_source.get('SiteName', 'N/A')
                project_size = project_source.get('ProjectSize', 'N/A')
                project_postcode = project_source.get('ProjectPostcode', 'N/A')
                role_locations_level2 = project_source.get('RoleLocationsLevel2', 'N/A')
                start_date = project_source.get('StartDate', 'N/A')
                sectors = project_source.get('Sectors', [])
                if sectors:
                    sector_values = ", ".join([sector.get('Sector', 'Unknown') for sector in sectors])
                else:
                    sector_values = 'N/A'
                
                email = 'N/A'
                
                print(f"Office Names: {office_names}")
                print(f"KeyCard Full Contact: {keycard_fullcontact}")
                print(f"Contract Stage: {contract_stage}")
                print(f"Site Name: {site_name}")
                print(f"Phone: {phone}")
                print(f"Email: {email}")
                print(f"Job Title: {job_title}")
                print(f"Project Size: {project_size}")
                print(f"Project Postcode: {project_postcode}")
                print(f"Role Locations Level 2: {role_locations_level2}")
                print(f"Start Date: {start_date}")
                print(f"Sectors: {sector_values}")
                print("-" * 40)
                 
                project_info = {
                    "Office Names": office_names,
                    "KeyCard Full Contact": keycard_fullcontact,
                    "Contract Stage": contract_stage,
                    "Site Name": site_name,
                    "Phone": phone,
                    "Email": email,
                    "Job Title": job_title,
                    "Project Size": project_size,
                    "Project Postcode": project_postcode,
                    "Role Locations Level 2": role_locations_level2,
                    "Start Date": start_date,
                    "Sectors": sector_values
                }
                
                projects_list.append(project_info)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

    try:
        with open('projects_data.txt', 'w') as file:
            for project in projects_list:
                file.write(json.dumps(project, indent=4))
                file.write('\n' + '-' * 40 + '\n')
        print("Data successfully written to projects_data.txt")
    except IOError as e:
        print(f"An error occurred while writing to file: {e}")

def job():
    print(f"Job started at {datetime.datetime.now()}")
    api_key = os.getenv("GLENIGAN_API_KEY")
    fetch_projects(api_key)
    print(f"Job completed at {datetime.datetime.now()}")


schedule.every().day.at("00:01").do(job)
def scheduled_task():
    file_path = 'projects_data.txt'
    projects_list = read_projects_from_file(file_path)
    process_projects(projects_list)

schedule.every().day.at("00:30").do(scheduled_task)

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(60)
