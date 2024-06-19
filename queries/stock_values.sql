--  % change of shares held between quarter with limits
with t1 as (
select a.cusip, max(a.nameofissuer) as ticker_name,
sum(case when b.reportcalendarorquarter = '2023-03-31' then sshprnamt else 0 end) AS q123_shares,
sum(case when b.reportcalendarorquarter = '2023-06-30' then sshprnamt else 0 end) AS q223_shares,
sum(case when b.reportcalendarorquarter = '2023-09-30' then sshprnamt else 0 end) AS q323_shares,
sum(case when b.reportcalendarorquarter = '2023-12-31' then sshprnamt else 0 end) AS q423_shares,
sum(case when b.reportcalendarorquarter = '2024-03-31' then sshprnamt else 0 end) AS q124_shares
from infotable a 
join reports_to_keep b 
on a.accession_number=b.accession_number
where b.reportcalendarorquarter>='2023-03-31'
and sshprnamttype = 'SH' and putcall is null
group by 1
)
select * from t1
where q123_shares > 0 and q223_shares > 0 and q323_shares > 0 and q423_shares > 0 and q124_shares > 0 -- make sure stock not new
and (q423_shares - q323_shares)/(q323_shares+1) <= 10 -- limit to the upside
and q223_shares > q123_shares and q323_shares > q223_shares and q423_shares > q323_shares and q124_shares > q423_shares
-- order by (q124_shares - q423_shares)/(q423_shares+1) desc
order by (q423_shares - q323_shares)/(q323_shares+1) desc;

--  % change of number of investors between quarter with limits
with t1 as (
select a.cusip, max(a.nameofissuer) as ticker_name,
count(distinct case when b.reportcalendarorquarter = '2023-03-31' then cik else null end) AS q123_num_holders,
count(distinct case when b.reportcalendarorquarter = '2023-06-30' then cik else null end) AS q223_num_holders,
count(distinct case when b.reportcalendarorquarter = '2023-09-30' then cik else null end) AS q323_num_holders,
count(distinct case when b.reportcalendarorquarter = '2023-12-31' then cik else null end) AS q423_num_holders,
count(distinct case when b.reportcalendarorquarter = '2024-03-31' then cik else null end) AS q124_num_holders
from infotable a 
join reports_to_keep b 
on a.accession_number=b.accession_number
where b.reportcalendarorquarter>='2023-03-31'
and sshprnamttype = 'SH' and putcall is null
group by 1
)
select * from t1
where 1=1
-- and q123_num_holders > 0 and q223_num_holders > 0 and q323_num_holders > 0 and q423_num_holders > 0 and q124_num_holders > 0 -- make sure stock not new
-- and (q423_num_holders - q323_num_holders)/(q323_num_holders+1) <= 10 -- limit to the upside
-- and q223_num_holders > q123_num_holders and q323_num_holders > q223_num_holders and q423_num_holders > q323_num_holders
-- order by (q124_num_holders - q423_num_holders)/(q423_num_holders+1) desc
order by (q423_num_holders - q323_num_holders)/(q323_num_holders+1) desc
;
