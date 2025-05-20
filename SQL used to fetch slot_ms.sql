
SELECT
 user_email,
 query,
 start_time,
 round(total_slot_ms / timestamp_diff(current_timestamp,start_time,millisecond),2) slots,
 current_datetime,
 DATETIME_DIFF(current_timestamp,start_time,second) AS duration_secs,
 total_bytes_processed AS byte_used,
 'https://console.cloud.google.com/bigquery?project='||project_id||'&j=bq:us:'||ifnull(parent_job_id,job_id)||'&page=queryresults' Bqurl,
from 
<project_id>.`region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where state !='DONE' and 
date(creation_time) = current_date and
DATETIME_DIFF(current_timestamp,start_time,second)>300 --running from last 5 min
--below check for slots consumed more than 1500 
and round(total_slot_ms / timestamp_diff(current_timestamp,start_time,millisecond),2) >1500
group by 1,2,3,4,5,6,7,8,9 ;
