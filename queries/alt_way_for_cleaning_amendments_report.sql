-- alternative way to clean -more complicated and might be slightly wrong
with 
by_qtr_funds_with_amends as (
	select distinct a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city
	from coverpage a 
	join submission b
	on a.accession_number=b.accession_number
	where a.isamendment='Y' 
),
by_qtr_all_funds as (
	select distinct a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city
	from coverpage a 
	join submission b
	on a.accession_number=b.accession_number
),
by_qtr_funds_no_amends as (
	select distinct a.*
	from by_qtr_all_funds a 
	left join by_qtr_funds_with_amends b 
	on a.cik=b.cik
	where b.cik is null
),
	-- keep latest restatements and all new holdings, group by report quarters and fund.
by_qtr_funds_with_amends_final_reports as (
	select distinct a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city, a.reporttype,
	max(a.accession_number) over(partition by a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city) as accession_number
	-- ,'final_report' as report_type
	from coverpage a
	join submission b 
	on a.accession_number=b.accession_number
	join by_qtr_funds_with_amends f
	on f.cik=b.cik
	and f.reportcalendarorquarter=b.periodofreport
	where a.isamendment ='N' or a.isamendment is null or (a.isamendment ='Y' and a.amendmenttype='RESTATEMENT')
	
	union
	
	select distinct a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city, a.reporttype,
	max(a.accession_number) over(partition by a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city) as accession_number
	-- ,'new_holdings' as report_type
	from coverpage a
	join submission b 
	on a.accession_number=b.accession_number
	join by_qtr_funds_with_amends f
	on f.cik=b.cik
	and f.reportcalendarorquarter=b.periodofreport
	where (a.isamendment ='Y' and a.amendmenttype='NEW HOLDINGS')
),
	-- keep all reports in the no amends funds
by_qtr_funds_no_amends_final_reports as (
	select distinct a.reportcalendarorquarter, b.cik, a.filingmanager_name, a.filingmanager_city, a.reporttype, a.accession_number
	from coverpage a
	join submission b 
	on a.accession_number=b.accession_number
	join by_qtr_funds_no_amends f
	on f.cik=b.cik
	and f.reportcalendarorquarter=b.periodofreport
	
),
final_result as (
	select * from by_qtr_funds_with_amends_final_reports
	union
	select * from by_qtr_funds_no_amends_final_reports
)
	
select * from final_result where cik='0001367441' order by 1,2,3,4,5,6;	