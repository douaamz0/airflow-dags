from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import csv
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv

# Charger le fichier .env
load_dotenv('/opt/airflow/.env')


# Configuration
JIRA_URL_ISSUES = 'https://iitslab.atlassian.net/rest/api/3/search'
USERNAME = os.getenv("JIRA_USERNAME")
API_TOKEN = os.getenv("JIRA_API_TOKEN")
OUTPUT_PATH = '/opt/airflow/output/jira_issues.csv'

# Headers
HEADERS = {'Accept': 'application/json'}
# Auth
AUTH = HTTPBasicAuth(USERNAME, API_TOKEN)

def fetch_jira_issues():
    with open(OUTPUT_PATH, mode='w', newline='', encoding='utf-8') as issues_file:
        issues_writer = csv.writer(issues_file)
        issues_writer.writerow(['Project Key', 'Issue Key', 'Summary', 'Status', 'Assignee', 'Priority', 'Resolution', 'Issue Type', 'Creation Date', 'Resolution Date'])
        
        start_at = 0
        max_results = 100

        while True:
            params = {'jql': 'ORDER BY created ASC', 'startAt': start_at, 'maxResults': max_results}
            response = requests.get(JIRA_URL_ISSUES, headers=HEADERS, auth=AUTH, params=params)
            
            if response.status_code == 200:
                issues_data = response.json()
                issues = issues_data.get('issues', [])
                
                for issue in issues:
                    fields = issue['fields']
                    issues_writer.writerow([
                        fields['project']['key'],
                        issue['key'],
                        fields['summary'],
                        fields['status']['name'],
                        fields['assignee']['displayName'] if fields.get('assignee') else 'Unassigned',
                        fields['priority']['name'] if fields.get('priority') else 'None',
                        fields['resolution']['name'] if fields.get('resolution') else 'Unresolved',
                        fields['issuetype']['name'],
                        fields['created'],
                        fields.get('resolutiondate', 'Unresolved')
                    ])
                
                if len(issues) < max_results:
                    break
                start_at += max_results
            else:
                print(f"Failed to retrieve issues: {response.status_code} - {response.text}")
                break

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'jira_issue_extraction',
    default_args=default_args,
    description='Fetch Jira issues and store in CSV',
    schedule_interval=timedelta(days=1),  # Runs daily
    catchup=False
)

# Define task
fetch_jira_task = PythonOperator(
    task_id='fetch_jira_issues',
    python_callable=fetch_jira_issues,
    dag=dag
)

fetch_jira_task
