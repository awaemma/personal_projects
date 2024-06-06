--- IMAL Fact
with restriction as (

select distinct additional_reference, REMARKS, 

case when b.status in ('td','p') then 'restricted' else 'not restricted' end as restriction_status, 

row_number() over (partition by additional_reference order by starting_date desc) rn, REASON_CODE

from imal.dimamf a

left join imal.DimCTSSPCOND b on a.cif_sub_no = b.acc_cif and a.gl_code = b.acc_gl and a.branch_code = b.acc_br and a.sl_no = b.acc_sl and a.currency_code = b.acc_cy
where a.gl_code in (210153,210801,210805,210806,210110,210101,210201,210210,210155,210802,210154,210811,210812,210156,210814) and len(additional_reference) = 10

and a.status not in ('C','R') -- and year(a.date_opnd)>=2022

)

select distinct a.ADDITIONAL_REFERENCE account_id, DATE_OPND OPENINGDATE, 
case when CV_AVAIL_BAL < 0 then 'FUNDED' else 'NOT FUNDED' end Fund_Status, e.LONG_DESC_ENG description, 
CASE WHEN a.STATUS = 'A' THEN 'Active'
WHEN a.STATUS = 'I' THEN 'Inactive'
WHEN a.STATUS = 'S' THEN 'Suspended'
WHEN a.STATUS = 'D' THEN 'Deleted'
else 'Other'
end as Customer_Status, c.LONG_DESC_ENG product_desc,
BRANCHNAME Branch_Name,
case when f.REGION_CODE in (12,13,35,22,26,27) then 'NORTH-WEST'
when f.REGION_CODE in (3,20,6,37,30,31,9) then 'NORTH-EAST'
when f.REGION_CODE in (11,16,28,5) then 'NORTH-CENTRAL'
when f.region_code in (25,18,29,17,19,14,34) then 'SOUTH-WEST'
when f.region_code in (15) then 'LAGOS'
when f.region_code in (8) then 'ABUJA'
else 'S_SOUTH & S_EAST' 
end as Region, g.BRIEF_DESC_ENG State_Located,h.REASON_CODE POSTRESTRICT,CASE
WHEN a.STATUS = 'A' THEN 'Active'
WHEN a.STATUS = 'T' THEN 'Dormant'
WHEN a.STATUS = 'I' THEN 'Inactive'
WHEN a.STATUS = 'R' THEN 'Rejected'
WHEN a.STATUS = 'M' THEN 'Closure Approved'
WHEN a.STATUS = 'O' THEN 'Opened'
WHEN a.STATUS = 'C' THEN 'Closed'
WHEN a.STATUS = 'P' THEN 'Applied for closure'
WHEN a.STATUS = 'X' THEN 'To be rejected'
WHEN a.STATUS = 'S' THEN 'Suspended'
WHEN a.STATUS = 'D' THEN 'Deleted'
WHEN a.STATUS = 'F' THEN 'Offended'
WHEN a.STATUS = 'Q' THEN 'To Be Suspended'
WHEN a.STATUS = 'Y' THEN 'To Be Reactivated'
END AS Account_Status, a.entered_by SOURCE_CHANNEL
from imal.DimAmf a
left join imal.DimCifAddress b on a.CIF_SUB_NO = b.CIF_NO and b.default_add = '1' and b.line_no ='0' and b.COMP_CODE =1
left join imal.DimGEN_LEDGER c on a.GL_CODE = c.GL_CODE and c.COMP_CODE = 1
left join imal.dimcif d on a.CIF_SUB_NO = d.CIF_NO and d.COMP_CODE = 1
left join imal.DimRIFCTT e on d.CIF_TYPE = e.TYPE_CODE and e.COMP_CODE = 1
left join imal.DimNIB_BRANCHES f on a.BRANCH_CODE = f.BRANCH_CODE and f.COMP_CODE = 1
left join imal.DimRegions g on b.REGION = g.REGION_CODE
left join restriction h on a.ADDITIONAL_REFERENCE = h.ADDITIONAL_REFERENCE and rn = 1
where a.[STATUS] not in ('c','r') and len(a.ADDITIONAL_REFERENCE) = 10
and a.GL_CODE in (210153,210801,210805,210806,210110,210101,210201,210210,210155,210802,210154,210811,210812,210156,210814)
