
-- NOTES
-- 1. amendement can be new holding or restatement. 
-- 2. The first such filing is due within 45 days after the end of the fourth quarter of the calendar year, 
-- i.e., the quarter ending December 31 of the same calendar year that you meet the $100 million filing threshold. 
-- 3. The filing is due within 45 days after December 31, or, stated differently, by February 14 of the subsequent calendar year.
-- new holding only add new entry, while restatement relist everything.



-- EXAMPLE: Single table EDA
select * from infotable
where accession_number in ("0000004962-25-000018", "0000004962-25-000056")
order by accession_number;

SELECT * from coverpage
where accession_number in ("0000004962-25-000018", "0000004962-25-000056");

SELECT * from submission --indicate whether is amendment
where accession_number in ("0000004962-25-000018", "0000004962-25-000056");

SELECT * from summarypage  --number of stocks in the filing
where accession_number in ("0000004962-25-000018", "0000004962-25-000056");

SELECT * from signature  --not important
where accession_number in ("0000004962-25-000018", "0000004962-25-000056");
--cornell cusip ("0000315082-24-000002", "0000315082-24-000003") 
--and titleofclass IN ('COM', 'CL A', 'Stock', 'Common Stock', 'COMMON STOCK', 'COM NEW', 'COMMON', 'ETF', 'COM CL A', 'SHS', 'CMN')

-- EXAMPLE: report quarter and how many rows in each
select reportcalendarorquarter, count(*)
from coverpage group by 1 order by 2 desc;

-- EXAMPLE: report quarter and how many rows in each
select cusip, count(*)
from infotable group by 1 order by 2 desc;

-- EXAMPLE: report quarter and how many rows in each
select titleofclass, count(*)
from infotable group by 1 order by 2 desc;


-- EXAMPLE: find all holdings in filings by filers and report quarter of June 2024
select b.accession_number, c.cik, b.filingmanager_name, 
b.reportcalendarorquarter, c.filing_date, c.submissiontype, b.amendmenttype,
a.tableentrytotal, a.tablevaluetotal,
d.nameofissuer, d.titleofclass , d.cusip, d.value , d.sshprnamt, d.sshprnamttype 
from summarypage a
join coverpage b 
on a.accession_number=b.accession_number
join submission c
on a.accession_number=c.accession_number
join infotable d 
on a.accession_number=d.accession_number 
where 1=1
and reportcalendarorquarter ='30-JUN-2024' --'2024-06-30'
and amendmenttype in ("", 'NEW HOLDINGS')
and putcall is null
--and cusip = '364760108' -- use to find specific company
order by b.accession_number, filingmanager_name, filing_date desc; 


-- EXAMPLE: find filer info and filiings
select b.accession_number, c.cik, b.filingmanager_name, 
b.reportcalendarorquarter, c.filing_date, c.submissiontype, b.amendmenttype,
a.tableentrytotal, a.tablevaluetotal 
from summarypage a
join coverpage b 
on a.accession_number=b.accession_number
join submission c
on a.accession_number=c.accession_number
where 1=1
-- and filingmanager_name in (select filingmanager_name from coverpage where isamendment='Y')
-- and reportcalendarorquarter ='30-JUN-2024' --'2024-06-30'
and amendmenttype in ("", 'NEW HOLDINGS')
and putcall is null
order by b.accession_number, filingmanager_name, filing_date desc; 


--EXAMPLE: Number of funds that purchased call/put of a stock by quarter 
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

--EXAMPLE: to keep only the latest restatment report if exists and all new holdings report group by quarter
create table reports_to_keep as 
with final_result as (
	-- select the latest amendment if any
	select distinct a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city, a.reporttype,
	max(a.accession_number) over(partition by a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city) as accession_number
	-- ,'final_report' as report_type
	from coverpage a
	join submission b 
	on a.accession_number=b.accession_number
	where a.isamendment ='N' or a.isamendment is null or (a.isamendment ='Y' and a.amendmenttype='RESTATEMENT')
	
	union
	
	-- aggregate with the new holdings added if any
	select distinct a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city, a.reporttype,
	max(a.accession_number) over(partition by a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city) as accession_number
	-- ,'new_holdings' as report_type
	from coverpage a
	join submission b 
	on a.accession_number=b.accession_number
	where (a.isamendment ='Y' and a.amendmenttype='NEW HOLDINGS')
)

select * from final_result;	