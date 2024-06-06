WITH inward_con AS (SELECT DISTINCT [SessionId]
      ,[Transnature]
      ,[ChannelCode]
      ,[BatchNumber]
      ,[PaymentRef]
      ,[MandateRefNum]
      ,[BillerID]
      ,[BillerName]
      ,[SenderBankCode]
      ,[SenderBank]
      ,[SenderAccount]
      ,[SenderName]
      ,[Amount]
      ,[Feecharge]
      ,[BranchCode]
      ,[CustomerNumber]
      ,[CurrencyCode]
      ,[LedCode]
      ,[SubAccountCode]
      ,[BeneficiaryAccountName]
      ,[Remark]
      ,[InputDate]
      ,[ApprovedDate]
      ,[Approvevalue]
      ,[TransactionType]
      ,[ResponseCode]
      ,[ResponseMessage]
      ,[FTadvice]
      ,[FTadviceDate]
      ,[TransactionProcessed]
      ,[TransactionProcessedDate]
      ,[ReversalStatus]
      ,[StaggingStatus]
      ,[TransactionStatus]
      ,[RepairStatus]
      ,[ExceedThreshold]
      ,[ExceedThresholddate]
      ,[CountTrasactionFreq]
      ,[Errcode]
      ,[Requery]
      ,[RequeryTimeSent]
      ,[RequeryTimeReceived]
      ,[InwardType]
      ,[NameEnquiryRef]
      ,[BeneficiaryBankVerificationNumber]
      ,[OriginatorAccountNumber]
      ,[OriginatorBankVerificationNumber]
      ,[OriginatorKYCLevel]
      ,[TransactionLocation]
      ,[BeneficiaryAccountNumber]
      ,[SettleFlag]
      ,[SettleDiff]
      ,[SettleDate]
      ,[SettleRemark]
      ,[SwitchDate]
      ,[VtellerPrinRsp]
      ,[FeeRsp]
      ,[VatRsp]
      ,[OriginalInputdate]
      ,[Statusflag]
      ,[AccountStatus]
      ,[Restriction]
      ,[AccountDescp]
      ,[DateReactivated]
      ,[BalanceAtTransTime]
      ,[ReasonFlag]
      ,[TSQcount]
      ,[SendersEmail]
      ,[SendersMobile]
      ,[SendersMobile2]
      ,[IsSterlingCustomer]
      ,[PickedStatus]
      ,[IsSent]
      ,[WalletAcct]
      ,[WalletAcctStatus]
      ,[ThirdpartyNuban]
      ,[ThirdpartyID]
      ,[PenalAmt]
      ,[GivingNg]
      ,[GivingNgAmount]
      ,[FTReference]
      ,[PAPSSResponseCode]
      ,[PAPSSResponseMessage]
      ,[TimeSent]
      ,[TimeReceived]
      --,[RefId]
  FROM [dbo].[NIPInboundTransactionTb_Batch] )


------------------------------conventional---------------------------------------------
Select transaction_date
      ,institution_code
	  ,product_code
	  ,product_subcategory_code
	  ,SUM(Count) count
	  ,status as status_txt
	  ,CASE WHEN status in ('CREDIT COMPLETED','Failed Credit','Pending Requery','Pending Credit') THEN 'Sterling'
			WHEN status like '%Sterling%' THEN 'Sterling'
			WHEN status like '%NIBSS%' THEN 'NIBSS'
			ELSE 'Undefined' 
			END as 'responsibility'
      ,ISNULL(SUM(Total_Processing_Time),0) AS processing_Time
	  ,ISNULL(SUM(timeto_send_requery),0) time_to_send_requery
	  ,ISNULL(SUM(requery_resp_time),0) AS response_time
	  ,ISNULL(SUM(tot_credit_time),0) AS credit_time
from
(
select 
CAST(inputdate as date) transaction_date,
'100' AS institution_code,
'010' AS product_code,
'011' AS product_subcategory_code,
Count(*) Count,
CASE WHEN Requery = '00' and TransactionProcessed = '1' THEN 'CREDIT COMPLETED'
WHEN Requery = '00' and (TransactionProcessed is null or TransactionProcessed = '0') and StaggingStatus != '102' THEN 'Pending Credit'
WHEN Requery = '00' and StaggingStatus = '102' THEN 'Failed Credit'
WHEN Requery is null THEN 'Pending Requery'
WHEN Requery != '00' and Requery in ('25', '26') THEN CONCAT('Failed Requery(NIBSS-', Requery, ')')
WHEN Requery != '00' and Requery not in ('25', '26') THEN CONCAT('Failed Requery(Sterling-', Requery, ')')
END As Status,
Sum(cast(DATEDIFF(second,originalinputdate,TransactionProcessedDate)As bigint)) AS Total_Processing_Time,
Sum(cast(DATEDIFF(second,originalinputdate,RequeryTimeSent) AS bigint)) AS Timeto_Send_Requery,
Sum(cast(DATEDIFF(second,RequeryTimeSent,RequeryTimeReceived) as bigint)) AS Requery_resp_time,
Sum(cast(DATEDIFF(second,RequeryTimeReceived,TransactionProcessedDate) as bigint)) AS Tot_Credit_time
from inward_con 
where CONVERT(DATE,inputdate) between '2024-06-02' and '2024-06-02'
and inwardtype in (1,5) and thirdpartynuban is null
group by CAST(inputdate as date), 
CASE WHEN Requery = '00' and TransactionProcessed = '1' THEN 'CREDIT COMPLETED'
WHEN Requery = '00' and (TransactionProcessed is null or TransactionProcessed = '0') and StaggingStatus != '102' THEN 'Pending Credit'
WHEN Requery = '00' and StaggingStatus = '102' THEN 'Failed Credit'
WHEN Requery is null THEN 'Pending Requery'
WHEN Requery != '00' and Requery in ('25', '26') THEN CONCAT('Failed Requery(NIBSS-', Requery, ')')
WHEN Requery != '00' and Requery not in ('25', '26') THEN CONCAT('Failed Requery(Sterling-', Requery, ')')
END ) A
Group by  transaction_date
      ,institution_code
	  ,product_code
	  ,product_subcategory_code
	  ,status
	  ,CASE WHEN status in ('CREDIT COMPLETED','Failed Credit','Pending Requery','Pending Credit') THEN 'Sterling'
			WHEN status like '%Sterling%' THEN 'Sterling'
			WHEN status like '%NIBSS%' THEN 'NIBSS'
			ELSE 'Undefined' 
			END
--Order by transaction_date