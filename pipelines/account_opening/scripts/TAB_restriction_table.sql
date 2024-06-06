--- IMAL RESTRICTION TABLE

select distinct additional_reference, REMARKS, 
case when b.status in ('td','p') then 'restricted' else 'not restricted' end as restriction_status,
REASON_CODE, -- this is the code you're asking for
row_number() over (partition by additional_reference order by starting_date desc) rn
from imal.dimamf a
left join imal.DimCTSSPCOND b on a.cif_sub_no = b.acc_cif and a.gl_code = b.acc_gl and a.branch_code = b.acc_br and a.sl_no = b.acc_sl and a.currency_code = b.acc_cy
where a.gl_code in (210153,210801,210805,210806,210110,210101,210201,210210,210155,210802,210154,210811,210812,210156,210814) and len(additional_reference) = 10
and a.status not in ('C','R')