
--Number of funds that purchased call/put of a stock by quarter 
with t1 as (
select a.cusip, max(a.nameofissuer) as ticker_name,
count(distinct case when b.reportcalendarorquarter = '2023-03-31' and putcall = 'Call' then cik else null end) AS q123_call_cnt,
count(distinct case when b.reportcalendarorquarter = '2023-06-30' and putcall = 'Call' then cik else null end) AS q223_call_cnt,
count(distinct case when b.reportcalendarorquarter = '2023-09-30' and putcall = 'Call' then cik else null end) AS q323_call_cnt,
count(distinct case when b.reportcalendarorquarter = '2023-12-31' and putcall = 'Call' then cik else null end) AS q423_call_cnt,
count(distinct case when b.reportcalendarorquarter = '2024-03-31' and putcall = 'Call' then cik else null end) AS q124_call_cnt,
count(distinct case when b.reportcalendarorquarter = '2023-03-31' and putcall = 'Put' then cik else null end) AS q123_put_cnt,
count(distinct case when b.reportcalendarorquarter = '2023-06-30' and putcall = 'Put' then cik else null end) AS q223_put_cnt,
count(distinct case when b.reportcalendarorquarter = '2023-09-30' and putcall = 'Put' then cik else null end) AS q323_put_cnt,
count(distinct case when b.reportcalendarorquarter = '2023-12-31' and putcall = 'Put' then cik else null end) AS q423_put_cnt,
count(distinct case when b.reportcalendarorquarter = '2024-03-31' and putcall = 'Put' then cik else null end) AS q124_put_cnt
from infotable a 
join reports_to_keep b 
on a.accession_number=b.accession_number
where extract(year from b.reportcalendarorquarter) > 2022
and sshprnamttype = 'SH' 
and putcall is not null
group by 1
)
select * from t1  
-- where q123_call_cnt > 10
order by q124_call_cnt/(q124_put_cnt+1)
;

