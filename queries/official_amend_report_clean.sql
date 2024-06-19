-- to keep only the latest restatment report if exists and all new holdings report group by quarter
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