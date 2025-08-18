-- MAIN EXAMPLE: find holdings and their relative changes by quarter
with raw_t1 as (
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
--and reportcalendarorquarter in ('31-MAR-2024', '30-JUN-2024', '30-SEP-2024', '31-DEC-2024','31-MAR-2025', '30-JUN-2025', '30-SEP-2025', '31-DEC-2025')
and reportcalendarorquarter in ('31-DEC-2024','31-MAR-2025', '30-JUN-2025', '30-SEP-2025', '31-DEC-2025')

and amendmenttype in ('', 'NEW HOLDINGS')
and putcall = ''
and d.sshprnamttype = 'SH'
and d.sshprnamt > 0
order by b.accession_number, filingmanager_name, filing_date desc),

name_mode AS (
  SELECT x.cusip, x.nameofissuer
  FROM raw_t1 x
  JOIN (
    SELECT cusip, MAX(cnt) AS max_cnt
    FROM (
      SELECT cusip, nameofissuer, COUNT(*) AS cnt
      FROM raw_t1
      GROUP BY cusip, nameofissuer
    ) t
    GROUP BY cusip
  ) y
  ON x.cusip = y.cusip
  GROUP BY x.cusip  -- keep one row per cusip
),

raw_t2 as (
select cusip, 
-- sum(case when reportcalendarorquarter = '31-MAR-2024' then sshprnamt else 0 end) as sh_2024q1,
--sum(case when reportcalendarorquarter = '30-JUN-2024' then sshprnamt else 0 end) as sh_2024q2,
--sum(case when reportcalendarorquarter = '30-SEP-2024' then sshprnamt else 0 end) as sh_2024q3,
sum(case when reportcalendarorquarter = '31-DEC-2024' then sshprnamt else 0 end) as sh_2024q4,
sum(case when reportcalendarorquarter = '31-MAR-2025' then sshprnamt else 0 end) as sh_2025q1,
--sum(case when reportcalendarorquarter = '30-JUN-2025' then sshprnamt else 0 end) as sh_2025q2,
--sum(case when reportcalendarorquarter = '30-SEP-2025' then sshprnamt else 0 end) as sh_2025q3,
--sum(case when reportcalendarorquarter = '31-DEC-2025' then sshprnamt else 0 end) as sh_2025q4

-- sum(case when reportcalendarorquarter = '31-MAR-2024' then value else 0 end) as value_2024q1,
--sum(case when reportcalendarorquarter = '30-JUN-2024' then value else 0 end) as value_2024q2,
--sum(case when reportcalendarorquarter = '30-SEP-2024' then value else 0 end) as value_2024q3,
sum(case when reportcalendarorquarter = '31-DEC-2024' then value else 0 end) as value_2024q4,
sum(case when reportcalendarorquarter = '31-MAR-2025' then value else 0 end) as value_2025q1
--sum(case when reportcalendarorquarter = '30-JUN-2025' then value else 0 end) as value_2025q2,
--sum(case when reportcalendarorquarter = '30-SEP-2025' then value else 0 end) as value_2025q3,
--sum(case when reportcalendarorquarter = '31-DEC-2025' then value else 0 end) as value_2025q4
from raw_t1
group by 1)

SELECT 
 coalesce(n.ticker, m.nameofissuer, 'N/A') as ticker, n.securityType, n.securityType2, 
  t.*,
  (CAST(t.sh_2025q1 AS DOUBLE) / NULLIF(CAST(t.sh_2024q4 AS DOUBLE), 0) - 1) AS pct_increase
FROM raw_t2 t
LEFT JOIN cusiptable n on t.cusip = n.cusip 
LEFT JOIN name_mode m on t.cusip = m.cusip
WHERE (CAST(t.sh_2025q1 AS DOUBLE) / NULLIF(CAST(t.sh_2024q4 AS DOUBLE), 0) - 1) BETWEEN -1.0 AND 5
  AND t.value_2024q4 > 100000000
  AND t.value_2025q1 > 100000000
ORDER BY pct_increase DESC;
 