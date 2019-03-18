select searchtype, dimensions, filter, result_rows, "date"
from (
  select gsc_property_job_id, sum(rows) as result_rows, "date"
  from query_queue
  group by gsc_property_job_id, "date") as q
join gsc_property_jobs as j on j.id = q.gsc_property_job_id
join gsc_properties as p on p.id = j.gsc_property_id
where account_name = 'serienjunkies'
order by "date" desc


select
  sum(CASE WHEN finished = 1 then 1 ELSE NULL END) as "finished",
  sum(CASE WHEN finished = 0 then 1 ELSE NULL END) as "not finished"
from query_queue


select gsc_property_job_id, searchtype, dimensions, filter
from query_queue as q
join gsc_property_jobs as j on j.id = q.gsc_property_job_id
group by gsc_property_job_id


select searchtype, dimensions, filter, result_rows
from (
  select gsc_property_job_id, sum(rows) as result_rows
  from query_queue
  group by gsc_property_job_id) as q
join gsc_property_jobs as j on j.id = q.gsc_property_job_id
order by "date" desc