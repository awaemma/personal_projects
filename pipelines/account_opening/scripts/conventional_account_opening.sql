----------Conventional Fact
SELECT accts.accountid, OPENINGDATE, 
CASE WHEN ABS(WORKINGBALANCE) > 0 THEN 'FUNDED'
     WHEN ABS(WORKINGBALANCE) = 0 and DATE_LAST_CR_CUST is null then 'never funded'
ELSE 'currently not FUNDED'
END AS Fund_Status,
ctype.description, ctype.CustomerType, cust.CUSTOMER_TYPE As Customer_Status, arrg.product_desc, 
bra.Branch_Name, bra.Region, bra.Zone, bra.State_Located,
accts.POSTRESTRICT, accts.STACODE As Account_Status, accts.SOURCE_CHANNEL
FROM [dbo].[DimAccountsV2] accts 
left join
[dbo].[DimCustomersFull] cust 
on accts.customer = cust.customerid 
left join
[dbo].[DimCustomerStatusV2] ctype
on cust.customer_status = ctype.CUSTOMER_STATUS
left join
[dbo].[DimAAArrangement] arrg
on accts.ARRANGEMENT_ID = arrg.arrangement_id
left join [dbo].[DimBranches] bra
on accts.COCODE = bra.Branch_code
--WHERE CONVERT(DATE, CONVERT(DATETIME, accts.DATE_TIME_CUSTOM, 102)) > = '2024-04-02'
WHERE CONVERT(DATE, OPENINGDATE)  between '2023-01-01' and cast(getdate() as date)  
and bra.Branch_type = 'FULL BRANCH' and accts.category in 
('1000','1001','1004','1006','1007','1008','1010','1013','1020','1022','1200',
  '1201','1302','1304','1305','1306','1307','1309','1501','1500','1502','1503','1504','1505',
  '1506','6400','1312','3157','3158','3159','6001','6002','6003','6005','6009','6010','6014',
  '6016','6017','6018','6019','6020','6027','6028','6029','6030','6031','6034','1507', '1783')
--these are list of actual savings account --> remember to always update this or better automate this clause
--)