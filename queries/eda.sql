-- get funds with amendment
select b.filingmanager_city, b.reporttype, c.filing_date, c.submissiontype, 
b.reportcalendarorquarter, b.filingmanager_name, b.isamendment, b.amendmenttype, a.* 
from summarypage a
join coverpage b 
on a.accession_number=b.accession_number
join submission c
on a.accession_number=c.accession_number
where filingmanager_name in (select filingmanager_name from coverpage where isamendment='Y')
-- and reportcalendarorquarter ='2023-06-30'
order by filingmanager_name, accession_number, amendmenttype desc;

-- collect fund, stock pair in chorono order
select b.filingmanager_name, a.nameofissuer, a.cusip, a.value, a.sshprnamt, a.sshprnamttype, a.accession_number, b.reportcalendarorquarter, b.accession_number 
from infotable a 
join reports_to_keep b 
on a.accession_number=b.accession_number
order by 1,2,3,4,5,6,8
limit 100;

select distinct titleofclass, sshprnamttype, putcall, count(*)
from infotable a 
join reports_to_keep b 
on a.accession_number=b.accession_number
where extract(year from b.reportcalendarorquarter) > 2022
group by 1,2,3 order by 4 desc;

--and titleofclass IN ('COM', 'CL A', 'Stock', 'Common Stock', 'COMMON STOCK', 'COM NEW', 'COMMON', 'ETF', 'COM CL A', 'SHS', 'CMN')

-- check by cusip shares by quarter
select * 
from infotable a 
join reports_to_keep b 
on a.accession_number=b.accession_number
where b.reportcalendarorquarter='2023-03-31'
and sshprnamttype = 'SH' and putcall is null;