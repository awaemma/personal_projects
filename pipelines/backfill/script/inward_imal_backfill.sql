WITH inward_Imal AS (SELECT DISTINCT [SessionId]
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
------------------------------------IMAL---------------------------------------------
Select transaction_date
      ,institution_code
	  ,product_code
	  ,product_subcategory_code
	  ,SUM(count) Count
	  ,Status status_txt
	  ,CASE WHEN status in ('CREDIT COMPLETED','Failed Credit','Pending Requery','Pending Credit') THEN 'Sterling'
			WHEN status like '%Sterling%' THEN 'Sterling'
			wHEN status like '%NIBSS%' THEN 'NIBSS'
			ELSE 'Undefined' 
			END AS 'Responsibility'
     , SUM(Total_Processing_Time) processing_Time
	 ,SUM(Timeto_Send_Requery) time_to_send_requery
	 ,SUM(Requery_resp_time) response_time
	 ,SUM(Tot_Credit_time) credit_time

from

(

select  CAST(inputdate AS DATE) transaction_date, 
		'100' AS institution_code,
		'010' AS product_code,
		'013' AS product_subcategory_code,
         count(*)Count, 
		CASE WHEN Requery = '00' and TransactionProcessed = '1' THEN 'CREDIT COMPLETED'
		WHEN Requery = '00' and (TransactionProcessed is null or TransactionProcessed = '0') and StaggingStatus != '102' THEN 'Pending Credit'
		WHEN Requery = '00' and StaggingStatus = '102' THEN 'Failed Credit'
		WHEN Requery is null THEN 'Pending Requery'
		WHEN Requery != '00' and Requery in ('25', '26') THEN CONCAT('Failed Requery(NIBSS-', Requery, ')')
		WHEN Requery != '00' and Requery not in ('25', '26') THEN CONCAT('Failed Requery(Sterling-', Requery, ')')
		END As Status,
		Sum(DATEDIFF(second,originalinputdate,TransactionProcessedDate)) AS Total_Processing_Time,
		Sum(DATEDIFF(second,originalinputdate,RequeryTimeSent)) AS Timeto_Send_Requery,
		Sum(DATEDIFF(second,RequeryTimeSent,RequeryTimeReceived)) AS Requery_resp_time,
		Sum(DATEDIFF(second,RequeryTimeReceived,TransactionProcessedDate)) AS Tot_Credit_time
from inward_Imal
where CONVERT(DATE,inputdate) between '2024-06-02' and '2024-06-02'
and inwardtype = 2
group by CAST(inputdate AS DATE), 
CASE WHEN Requery = '00' and TransactionProcessed = '1' THEN 'CREDIT COMPLETED'
WHEN Requery = '00' and (TransactionProcessed is null or TransactionProcessed = '0') and StaggingStatus != '102' THEN 'Pending Credit'
WHEN Requery = '00' and StaggingStatus = '102' THEN 'Failed Credit'
WHEN Requery is null THEN 'Pending Requery'
WHEN Requery != '00' and Requery in ('25', '26') THEN CONCAT('Failed Requery(NIBSS-', Requery, ')')
WHEN Requery != '00' and Requery not in ('25', '26') THEN CONCAT('Failed Requery(Sterling-', Requery, ')')
END ) A

group by transaction_date
      ,institution_code
	  ,product_code
	  ,product_subcategory_code
	  ,Status
	  ,CASE WHEN status in ('CREDIT COMPLETED','Failed Credit','Pending Requery','Pending Credit') THEN 'Sterling'
			WHEN status like '%Sterling%' THEN 'Sterling'
			wHEN status like '%NIBSS%' THEN 'NIBSS'
			ELSE 'Undefined' 
			END
order by transaction_date