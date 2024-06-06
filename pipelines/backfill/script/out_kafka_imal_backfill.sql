WITH outward_cte AS (SELECT DISTINCT  [SessionID]
      ,[NameEnquirySessionID]
      ,[TransactionCode]
      ,[ChannelCode]
      ,[PaymentReference]
      ,[Amount]
      ,[CreditAccountName]
      ,[CreditAccountNumber]
      ,[OriginatorName]
      ,[BranchCode]
      ,[CustomerID]
      ,[CurrencyCode]
      ,[LedgerCode]
      ,[SubAccountCode]
      ,[DateAdded]
      ,[DebitRequeryStatus]
      ,[NIBSSRequeryStatus]
      ,[NameEnquiryResponse]
      ,[LastUpdate]
      ,[ReversalStatus]
      ,[DebitResponse]
      ,[FTAdvice]
      ,[FTAdviceDate]
      ,[DebitAccountNumber]
      ,[BeneficiaryBankCode]
      ,[PrincipalResponse]
      ,[FeeResponse]
      ,[VatResponse]
      ,[AccountStatus]
      ,[Restriction]
      ,[StatusFlag]
      ,[FraudResponse]
      ,[FraudScore]
      ,[OriginatorBVN]
      ,[BeneficiaryBVN]
      ,[BeneficiaryKYCLevel]
      ,[OriginatorKYCLevel]
      ,[TransactionLocation]
      ,[AppID]
      ,[OriginatorEmail]
      ,[DebitServiceRequestTime]
      ,[DebitServiceResponseTime]
      ,[FundsTransferResponse]
      ,[KafkaStatus]
      ,[PriorityLevel]
      ,[NIBSSResponse]
      ,[OutwardTransactionType]
      ,[AppsTransactionType]
      ,[PrincipalResponseReversed]
      ,[FeeResponseReversed]
      ,[VatResponseReversed]
      ,[WalletAccountNumber]
      ,[IsWalletTransaction]
      ,[IsImalTransaction]
      ,[ImalServiceResponse]
      ,[DebitRequeryCounter]
      ,[ReversalRequeryCounter]
      ,[FraudRequeryResponse]
      FROM dbo.tbl_NIPOutwardTransactions_Batch 
	  WHERE -- dateadded between '2023-07-01 00:00:00' and '2023-12-06 23:59:59'
	  CAST(dateadded as date) between '2024-06-04' and '2024-06-04')


  Select transaction_date,
         institution_code,
		 product_code,
		 product_subcategory_code,
		 count,
		 value,
		 transaction_band,
		 status,
		 /**CASE WHEN respText in ('Beneficiary Bank not available', 'Timeout waiting for response from destination', 
			'Do not honor', 'Dormant Account', 'Request processing in progress', 'No action taken', 
			'Unable to locate record','Security violation') THEN 'Receiving Bank'
			WHEN respText in ('Invalid Batch Number', 'Unknown Bank Code', 'Format error', 'System malfunction', 
			'Duplicate record') THEN 'NIBSS Error'
			WHEN respText in ('Transaction not permitted to sender', 'No sufficient funds', 'Transfer limit Exceeded',
			'Amount not allowed on savings account', 'Invalid Account', 'Invalid Amount', 'Invalid transaction', 'Account Name Mismatch',
			'Please contact customer care unit.', '') THEN 'Customer'
			WHEN respText like '%POSTING.RESTRICT%' THEN 'Customer'
			ELSE 'Sterling - Outward' END AS 'Responsibility', **/
		 StatusFlag as status_num,
		 response_time,
		 AppID as app_id
from

  (
  select CAST(dateadded as date) transaction_date, 
       '101' AS institution_code,
		'020' AS product_code,
		'024' AS product_subcategory_code,
		count(*) Count,
		sum(Amount) Value,
		CASE
			WHEN Amount <= 5000 THEN '<5000'
			WHEN Amount > 5000 and Amount <=50000 THEN '5001 - 50000'
			WHEN Amount > 50000 THEN '>50000'
			END AS Transaction_band,
       CASE WHEN StatusFlag = 7 and NIBSSResponse = '00' THEN 'Success'
			WHEN StatusFlag = 3 THEN 'Debit/Credit Processing'
			WHEN StatusFlag = 8 and kafkastatus != 'Processed' THEN CONCAT('Failed - ', NIBSSResponse, ' (', kafkastatus, '-', reversalstatus, ')')
			WHEN StatusFlag = 8 THEN CONCAT('Failed - ', NIBSSResponse, ' (', reversalstatus, ')')
			WHEN StatusFlag = 100 THEN CONCAT('Failed - ', NIBSSResponse, ' (', 'pending reversal - Waiting for NIBSS Settlement', ')')
			WHEN StatusFlag = 9 and NIBSSResponse is null and NIBSSRequeryStatus is null THEN Kafkastatus
			WHEN StatusFlag = 9 and kafkastatus ='Sent For Reversal' and FraudResponse is NULL THEN CONCAT('Null Resp. from Fraud -', kafkastatus)
			WHEN StatusFlag = 11 THEN 'Failed fraud analysis'
			WHEN StatusFlag in (26, 27) and AppId in ('45') THEN 'Debit unsuccessful - vteller'
			WHEN StatusFlag in (26, 27) and AppId in ('106') THEN 'Debit unsuccessful - IMAL SOA'
			ELSE 'Others'
			END AS Status,
			r.respText,
		    StatusFlag,
			sum(DATEDIFF(second,dateadded,lastupdate)) As Response_Time,
			AppId
from outward_cte a
join tbl_responseCodes r on a.NIBSSResponse = r.respCode
-- where CAST(dateadded as date) between '2023-07-11' and '2023-07-20'
and DebitAccountNumber like '05%'
GROUP BY CAST(dateadded as date),
         CASE
			WHEN Amount <= 5000 THEN '<5000'
			WHEN Amount > 5000 and Amount <=50000 THEN '5001 - 50000'
			WHEN Amount > 50000 THEN '>50000'
			END,
			r.respText,
			StatusFlag,
       CASE WHEN StatusFlag = 7 and NIBSSResponse = '00' THEN 'Success'
			WHEN StatusFlag = 3 THEN 'Debit/Credit Processing'
			WHEN StatusFlag = 8 and kafkastatus != 'Processed' THEN CONCAT('Failed - ', NIBSSResponse, ' (', kafkastatus, '-', reversalstatus, ')')
			WHEN StatusFlag = 8 THEN CONCAT('Failed - ', NIBSSResponse, ' (', reversalstatus, ')')
			WHEN StatusFlag = 100 THEN CONCAT('Failed - ', NIBSSResponse, ' (', 'pending reversal - Waiting for NIBSS Settlement', ')')
			WHEN StatusFlag = 9 and NIBSSResponse is null and NIBSSRequeryStatus is null THEN Kafkastatus
			WHEN StatusFlag = 9 and kafkastatus ='Sent For Reversal' and FraudResponse is NULL THEN CONCAT('Null Resp. from Fraud -', kafkastatus)
			WHEN StatusFlag = 11 THEN 'Failed fraud analysis'
			WHEN StatusFlag in (26, 27) and AppId in ('45') THEN 'Debit unsuccessful - vteller'
			WHEN StatusFlag in (26, 27) and AppId in ('106') THEN 'Debit unsuccessful - IMAL SOA'
			ELSE 'Others'
			END,
			AppId ) A
ORDER BY transaction_date