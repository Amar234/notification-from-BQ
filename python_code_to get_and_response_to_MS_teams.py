from google.cloud import bigquery
import requests
import json
from datetime import datetime, timedelta

# Set your BigQuery project ID, dataset ID, and table ID
project_id = 'project-id'

# Set your Microsoft Teams webhook URL
#Update the teams webhook url as created in 1st steps
teams_webhook_url = "https://outlook.office.com/webhook/your_webhook_url"

def get_long_running_queries(project_id, threshold_duration, threshold_slots):
 client = bigquery.Client(project=project_id)

 # Calculate the timestamp for 5 minutes ago
 five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)

 # Query to get long-running queries
 query = f"""
SELECT
user_email,
query,
start_time,
round(total_slot_ms / timestamp_diff(current_timestamp,start_time,millisecond),2) slots,
current_datetime,
DATETIME_DIFF(current_timestamp,start_time,second) AS duration_secs,
total_bytes_processed  AS byte_used,
'https://console.cloud.google.com/bigquery?project='||project_id||'&j=bq:us:'||ifnull(parent_job_id,job_id)||'&page=queryresults' Bqurl,
from 
<project_id>.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where state !='DONE' and 
date(creation_time) = current_date and
DATETIME_DIFF(current_timestamp,start_time,second)>300 --running from last 5 min
--below check for slots consumed more than 1500 
and round(total_slot_ms / timestamp_diff(current_timestamp,start_time,millisecond),2) >1500
group by 1,2,3,4,5,6,7,8,9

"""

 # Run the query
 query_job = client.query(query)

 # Fetch results
 results = query_job.result()

 return results

def send_teams_message(webhook_url, message):
 headers = {"Content-Type": "application/json"}
 payload = {
 "text": message
 }

 response = requests.post(webhook_url, headers=headers, data=json.dumps(payload))

 if response.status_code == 200:
 print("Message sent successfully")
 else:
 print(f"Failed to send message. Status code: {response.status_code}, Response content: {response.text}")

def main():
 # Set threshold values
 threshold_duration = 5 * 60 # 5 minutes in seconds
 threshold_slots = 1500

 # Get long-running queries
 long_running_queries = get_long_running_queries(project_id, threshold_duration, threshold_slots)

 # Format the message
 message = "**Long running queries:**\n\n"
 for row in long_running_queries:
 message += f"**Job Executed By:-** {row['user_email']}\n```sql\n{row['query']}\n``` \n**Start Time:-** {row['start_time']}\n**Total Slots:-** {row['slots']}\n\n**Job Details:-** {row['Bqurl']} \n\n"

 # Send the message to Teams
 send_teams_message(teams_webhook_url, message)

if __name__ == "__main__":
 main()

