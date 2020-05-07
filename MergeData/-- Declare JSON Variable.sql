
-- Declare JSON Variable
declare @json nvarchar(max) = 
                         (
                             SELECT
    CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData
FROM
    OPENROWSET(BULK 'oakpoint-data/D18336/claims.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                         );

-- Declare Temp Table
Declare @TableView TABLE 
                         (
    ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
    col_href varchar(8000),
    col_claim_sr_no varchar(8000),
    col_claim_description_id varchar(8000),
    col_patient_id varchar(8000),
    col_patient varchar(8000),
    col_guarantor_id varchar(8000),
    col_guarantor varchar(8000),
    col_provider_id varchar(8000),
    col_provider varchar(8000),
    col_claim_sent_date varchar(8000),
    col_total_billed_amount varchar(8000),
    col_estimated_amount varchar(8000),
    col_claim_payment_date varchar(8000),
    col_payment_amount varchar(8000),
    col_claim_status varchar(8000),
    col_primary_or_secondary varchar(8000),
    col_insurance_company_id varchar(8000),
    col_insurance_company varchar(8000),
    col_practice_id varchar(8000),
    col_practice varchar(8000),
    col_claim_description varchar(8000),
    col_insurance_company_name varchar(8000),
    col_claim_channel varchar(8000),
    col_note varchar(8000),
    col_rendering_provider varchar(8000),
    col_payer_id varchar(8000),
    col_tracer varchar(8000),
    col_on_hold_date varchar(8000),
    col_resent_date varchar(8000),
    col_creation_date varchar(8000),
    col_total_paid_amount varchar(8000),
    col_cheque_no varchar(8000),
    col_bank_no varchar(8000),
    col_standard varchar(8000),
    col_preventive varchar(8000),
    col_others varchar(8000),
    col_pay_to_provider varchar(8000),
    col_tp varchar(8000),
    created_at datetime,
    updated_at datetime,
    office_id varchar(2000),
    practice_name varchar(2000)
                         );

INSERT INTO @TableView
    ([col_href],[col_claim_sr_no],[col_claim_description_id],[col_patient_id],[col_patient],[col_guarantor_id],[col_guarantor],[col_provider_id],[col_provider],[col_claim_sent_date],[col_total_billed_amount],[col_estimated_amount],[col_claim_payment_date],[col_payment_amount],[col_claim_status],[col_primary_or_secondary],[col_insurance_company_id],[col_insurance_company],[col_practice_id],[col_practice],[col_claim_description],[col_insurance_company_name],[col_claim_channel],[col_note],[col_rendering_provider],[col_payer_id],[col_tracer],[col_on_hold_date],[col_resent_date],[col_creation_date],[col_total_paid_amount],[col_cheque_no],[col_bank_no],[col_standard],[col_preventive],[col_others],[col_pay_to_provider],[col_tp],created_at, updated_at, office_id, practice_name)
SELECT *,
    '2020-05-07T03:05:24.575Z',
    '2020-05-07T03:05:24.575Z',
    'D18336',
    'west cary dental'
FROM OPENJSON(@json)  
                             WITH  
                             (
                                col_href varchar(8000) '$.href',col_claim_sr_no varchar(8000) '$.claim_sr_no',col_claim_description_id varchar(8000) '$.claim_description_id',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_guarantor_id varchar(8000) '$.guarantor_id',col_guarantor varchar(8000) '$.guarantor',col_provider_id varchar(8000) '$.provider_id',col_provider varchar(8000) '$.provider',col_claim_sent_date varchar(8000) '$.claim_sent_date',col_total_billed_amount varchar(8000) '$.total_billed_amount',col_estimated_amount varchar(8000) '$.estimated_amount',col_claim_payment_date varchar(8000) '$.claim_payment_date',col_payment_amount varchar(8000) '$.payment_amount',col_claim_status varchar(8000) '$.claim_status',col_primary_or_secondary varchar(8000) '$.primary_or_secondary',col_insurance_company_id varchar(8000) '$.insurance_company_id',col_insurance_company varchar(8000) '$.insurance_company',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_claim_description varchar(8000) '$.claim_description',col_insurance_company_name varchar(8000) '$.insurance_company_name',col_claim_channel varchar(8000) '$.claim_channel',col_note varchar(8000) '$.note',col_rendering_provider varchar(8000) '$.rendering_provider',col_payer_id varchar(8000) '$.payer_id',col_tracer varchar(8000) '$.tracer',col_on_hold_date varchar(8000) '$.on_hold_date',col_resent_date varchar(8000) '$.resent_date',col_creation_date varchar(8000) '$.creation_date',col_total_paid_amount varchar(8000) '$.total_paid_amount',col_cheque_no varchar(8000) '$.cheque_no',col_bank_no varchar(8000) '$.bank_no',col_standard varchar(8000) '$.standard',col_preventive varchar(8000) '$.preventive',col_others varchar(8000) '$.others',col_pay_to_provider varchar(8000) '$.pay_to_provider',col_tp varchar(8000) '$.tp'
                             )

select count(*) as preDelete
from @TableView;


MERGE claims original
                         USING @TableView modified
                         ON original.col_href = modified.col_href
                         WHEN MATCHED 
                             THEN UPDATE SET 
                             original.[col_href] = modified.[col_href],original.[col_claim_sr_no] = modified.[col_claim_sr_no],original.[col_claim_description_id] = modified.[col_claim_description_id],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_guarantor] = modified.[col_guarantor],original.[col_provider_id] = modified.[col_provider_id],original.[col_provider] = modified.[col_provider],original.[col_claim_sent_date] = modified.[col_claim_sent_date],original.[col_total_billed_amount] = modified.[col_total_billed_amount],original.[col_estimated_amount] = modified.[col_estimated_amount],original.[col_claim_payment_date] = modified.[col_claim_payment_date],original.[col_payment_amount] = modified.[col_payment_amount],original.[col_claim_status] = modified.[col_claim_status],original.[col_primary_or_secondary] = modified.[col_primary_or_secondary],original.[col_insurance_company_id] = modified.[col_insurance_company_id],original.[col_insurance_company] = modified.[col_insurance_company],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_claim_description] = modified.[col_claim_description],original.[col_insurance_company_name] = modified.[col_insurance_company_name],original.[col_claim_channel] = modified.[col_claim_channel],original.[col_note] = modified.[col_note],original.[col_rendering_provider] = modified.[col_rendering_provider],original.[col_payer_id] = modified.[col_payer_id],original.[col_tracer] = modified.[col_tracer],original.[col_on_hold_date] = modified.[col_on_hold_date],original.[col_resent_date] = modified.[col_resent_date],original.[col_creation_date] = modified.[col_creation_date],original.[col_total_paid_amount] = modified.[col_total_paid_amount],original.[col_cheque_no] = modified.[col_cheque_no],original.[col_bank_no] = modified.[col_bank_no],original.[col_standard] = modified.[col_standard],original.[col_preventive] = modified.[col_preventive],original.[col_others] = modified.[col_others],original.[col_pay_to_provider] = modified.[col_pay_to_provider],original.[col_tp] = modified.[col_tp],
                         original.updated_at = '2020-05-07T03:05:24.575Z',
                         original.office_id = 'D18336',
                         practice_name = 'west cary dental'
                         WHEN NOT MATCHED BY TARGET THEN
                         INSERT  
                         (
                             [col_href],[col_claim_sr_no],[col_claim_description_id],[col_patient_id],[col_patient],[col_guarantor_id],[col_guarantor],[col_provider_id],[col_provider],[col_claim_sent_date],[col_total_billed_amount],[col_estimated_amount],[col_claim_payment_date],[col_payment_amount],[col_claim_status],[col_primary_or_secondary],[col_insurance_company_id],[col_insurance_company],[col_practice_id],[col_practice],[col_claim_description],[col_insurance_company_name],[col_claim_channel],[col_note],[col_rendering_provider],[col_payer_id],[col_tracer],[col_on_hold_date],[col_resent_date],[col_creation_date],[col_total_paid_amount],[col_cheque_no],[col_bank_no],[col_standard],[col_preventive],[col_others],[col_pay_to_provider],[col_tp],created_at, updated_at, office_id, practice_name
                         )
                         VALUES
                         (
                             modified.[col_href],modified.[col_claim_sr_no],modified.[col_claim_description_id],modified.[col_patient_id],modified.[col_patient],modified.[col_guarantor_id],modified.[col_guarantor],modified.[col_provider_id],modified.[col_provider],modified.[col_claim_sent_date],modified.[col_total_billed_amount],modified.[col_estimated_amount],modified.[col_claim_payment_date],modified.[col_payment_amount],modified.[col_claim_status],modified.[col_primary_or_secondary],modified.[col_insurance_company_id],modified.[col_insurance_company],modified.[col_practice_id],modified.[col_practice],modified.[col_claim_description],modified.[col_insurance_company_name],modified.[col_claim_channel],modified.[col_note],modified.[col_rendering_provider],modified.[col_payer_id],modified.[col_tracer],modified.[col_on_hold_date],modified.[col_resent_date],modified.[col_creation_date],modified.[col_total_paid_amount],modified.[col_cheque_no],modified.[col_bank_no],modified.[col_standard],modified.[col_preventive],modified.[col_others],modified.[col_pay_to_provider],modified.[col_tp],
                         '2020-05-07T03:05:24.575Z',
                         '2020-05-07T03:05:24.575Z',
                         'D18336',
                         'west cary dental'
                         );





select
    P.spid
, right(convert(varchar, 
            dateadd(ms, datediff(ms, P.last_batch, getdate()), '1900-01-01'), 
            121), 12) as 'batch_duration'
, P.program_name
, P.hostname
, P.loginame
from Practice_Data.dbo.sysprocesses P
where P.spid > 50
    and P.status not in ('background', 'sleeping')
    and P.cmd not in ('AWAITING COMMAND'
                    ,'MIRROR HANDLER'
                    ,'LAZY WRITER'
                    ,'CHECKPOINT SLEEP'
                    ,'RA MANAGER')
order by batch_duration desc


declare
    @spid int
,   @stmt_start int
,   @stmt_end int
,   @sql_handle binary(20)

set @spid = 75
-- Fill this in

select top 1
    @sql_handle = sql_handle
, @stmt_start = case stmt_start when 0 then 0 else stmt_start / 2 end
, @stmt_end = case stmt_end when -1 then -1 else stmt_end / 2 end
from Practice_Data.dbo.sysprocesses
where   spid = @spid
order by ecid

SELECT
    SUBSTRING(  text,
            COALESCE(NULLIF(@stmt_start, 0), 1),
            CASE @stmt_end
                WHEN -1
                    THEN DATALENGTH(text)
                ELSE
                    (@stmt_end - @stmt_start)
                END
        )
FROM ::fn_get_sql(@sql_handle)