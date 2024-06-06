WITH inward_fin AS (SELECT DISTINCT [SessionId]
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

--------------------------------------FINTECH------------------------------------------
Select transaction_date,
       institution_code,
	   product_code,
	   product_subcategory_code,
	   count,
	   status as status_txt,
	   CASE WHEN status in ('CREDIT COMPLETED','Failed Credit','Pending Credit','Pending Requery') THEN 'Sterling'
		    WHEN status like '%Sterling%' THEN 'Sterling'
			WHEN status like '%NIBSS%' THEN 'NIBSS'
			ELSE 'Undefined' 
			END AS 'responsibility',
			Total_Processing_Time as processing_time,
			Timeto_Send_Requery as time_to_send_requery,
			Requery_resp_time as response_time,
			Tot_Credit_time as credit_time

from
(

select Cast(inputdate as date) transaction_date,
       '100' AS institution_code,
		'010' AS product_code,
		'012' AS product_subcategory_code,
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
from inward_fin
where CONVERT(DATE,inputdate) between '2024-06-02' and '2024-06-02' and thirdpartyid=2
group by Cast(inputdate as date),
         CASE WHEN Requery = '00' and TransactionProcessed = '1' THEN 'CREDIT COMPLETED'
		WHEN Requery = '00' and (TransactionProcessed is null or TransactionProcessed = '0') and StaggingStatus != '102' THEN 'Pending Credit'
		WHEN Requery = '00' and StaggingStatus = '102' THEN 'Failed Credit'
		WHEN Requery is null THEN 'Pending Requery'
		WHEN Requery != '00' and Requery in ('25', '26') THEN CONCAT('Failed Requery(NIBSS-', Requery, ')')
		WHEN Requery != '00' and Requery not in ('25', '26') THEN CONCAT('Failed Requery(Sterling-', Requery, ')')
		END
         
		 ) A