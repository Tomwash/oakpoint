
-- Declare JSON Variable
declare @json nvarchar(max) = 
                        (
                            SELECT
    CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData
FROM
    OPENROWSET(BULK 'oakpoint-data/D18336/appointments.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        );

-- Declare Temp Table
Declare @TableView TABLE 
                        (
    ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
    col_href varchar(8000),
    col_appointment_sr_no varchar(8000),
    col_patient_id varchar(8000),
    col_patient varchar(8000),
    col_operatory varchar(8000),
    col_date varchar(8000),
    col_time varchar(8000),
    col_length varchar(8000),
    col_description varchar(8000),
    col_amount varchar(8000),
    col_status varchar(8000),
    col_last_changed_date varchar(8000),
    col_appointment_made_date varchar(8000),
    col_type varchar(8000),
    col_procedure_code1 varchar(8000),
    col_procedure_code1_amount varchar(8000),
    col_procedure_code2 varchar(8000),
    col_procedure_code2_amount varchar(8000),
    col_procedure_code3 varchar(8000),
    col_procedure_code3_amount varchar(8000),
    col_procedure_code4 varchar(8000),
    col_procedure_code4_amount varchar(8000),
    col_procedure_code5 varchar(8000),
    col_procedure_code5_amount varchar(8000),
    col_procedure_code6 varchar(8000),
    col_procedure_code6_amount varchar(8000),
    col_procedure_code7 varchar(8000),
    col_procedure_code7_amount varchar(8000),
    col_hygienist varchar(8000),
    col_provider_id varchar(8000),
    col_provider varchar(8000),
    col_practice_id varchar(8000),
    col_practice varchar(8000),
    col_is_companion varchar(8000),
    col_sooner_if_possible varchar(8000),
    col_confirmed_on_date varchar(8000),
    col_note varchar(8000),
    col_patient_name varchar(8000),
    col_guarantor_id varchar(8000),
    col_guarantor_name varchar(8000),
    col_procedure_code1_time varchar(8000),
    col_procedure_code2_time varchar(8000),
    col_procedure_code3_time varchar(8000),
    col_procedure_code4_time varchar(8000),
    col_procedure_code5_time varchar(8000),
    col_procedure_code6_time varchar(8000),
    col_procedure_code7_time varchar(8000),
    col_tooth_number varchar(8000),
    col_surface_quadrant varchar(8000),
    col_surface_quadrant_type varchar(8000),
    col_schedule varchar(8000),
    col_treatment_class varchar(8000),
    col_cancelled varchar(8000),
    col_procedure_codes varchar(8000),
    created_at datetime,
    updated_at datetime,
    office_id varchar(2000),
    practice_name varchar(2000)
                        );

INSERT INTO @TableView
    ([col_href],[col_appointment_sr_no],[col_patient_id],[col_patient],[col_operatory],[col_date],[col_time],[col_length],[col_description],[col_amount],[col_status],[col_last_changed_date],[col_appointment_made_date],[col_type],[col_procedure_code1],[col_procedure_code1_amount],[col_procedure_code2],[col_procedure_code2_amount],[col_procedure_code3],[col_procedure_code3_amount],[col_procedure_code4],[col_procedure_code4_amount],[col_procedure_code5],[col_procedure_code5_amount],[col_procedure_code6],[col_procedure_code6_amount],[col_procedure_code7],[col_procedure_code7_amount],[col_hygienist],[col_provider_id],[col_provider],[col_practice_id],[col_practice],[col_is_companion],[col_sooner_if_possible],[col_confirmed_on_date],[col_note],[col_patient_name],[col_guarantor_id],[col_guarantor_name],[col_procedure_code1_time],[col_procedure_code2_time],[col_procedure_code3_time],[col_procedure_code4_time],[col_procedure_code5_time],[col_procedure_code6_time],[col_procedure_code7_time],[col_tooth_number],[col_surface_quadrant],[col_surface_quadrant_type],[col_schedule],[col_treatment_class],[col_cancelled],[col_procedure_codes],created_at, updated_at, office_id, practice_name)
SELECT *,
    '2020-05-12T01:02:40.975Z',
    '2020-05-12T01:02:40.975Z',
    'D18336',
    'west cary dental'
FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_appointment_sr_no varchar(8000) '$.appointment_sr_no',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_operatory varchar(8000) '$.operatory',col_date varchar(8000) '$.date',col_time varchar(8000) '$.time',col_length varchar(8000) '$.length',col_description varchar(8000) '$.description',col_amount varchar(8000) '$.amount',col_status varchar(8000) '$.status',col_last_changed_date varchar(8000) '$.last_changed_date',col_appointment_made_date varchar(8000) '$.appointment_made_date',col_type varchar(8000) '$.type',col_procedure_code1 varchar(8000) '$.procedure_code1',col_procedure_code1_amount varchar(8000) '$.procedure_code1_amount',col_procedure_code2 varchar(8000) '$.procedure_code2',col_procedure_code2_amount varchar(8000) '$.procedure_code2_amount',col_procedure_code3 varchar(8000) '$.procedure_code3',col_procedure_code3_amount varchar(8000) '$.procedure_code3_amount',col_procedure_code4 varchar(8000) '$.procedure_code4',col_procedure_code4_amount varchar(8000) '$.procedure_code4_amount',col_procedure_code5 varchar(8000) '$.procedure_code5',col_procedure_code5_amount varchar(8000) '$.procedure_code5_amount',col_procedure_code6 varchar(8000) '$.procedure_code6',col_procedure_code6_amount varchar(8000) '$.procedure_code6_amount',col_procedure_code7 varchar(8000) '$.procedure_code7',col_procedure_code7_amount varchar(8000) '$.procedure_code7_amount',col_hygienist varchar(8000) '$.hygienist',col_provider_id varchar(8000) '$.provider_id',col_provider varchar(8000) '$.provider',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_is_companion varchar(8000) '$.is_companion',col_sooner_if_possible varchar(8000) '$.sooner_if_possible',col_confirmed_on_date varchar(8000) '$.confirmed_on_date',col_note varchar(8000) '$.note',col_patient_name varchar(8000) '$.patient_name',col_guarantor_id varchar(8000) '$.guarantor_id',col_guarantor_name varchar(8000) '$.guarantor_name',col_procedure_code1_time varchar(8000) '$.procedure_code1_time',col_procedure_code2_time varchar(8000) '$.procedure_code2_time',col_procedure_code3_time varchar(8000) '$.procedure_code3_time',col_procedure_code4_time varchar(8000) '$.procedure_code4_time',col_procedure_code5_time varchar(8000) '$.procedure_code5_time',col_procedure_code6_time varchar(8000) '$.procedure_code6_time',col_procedure_code7_time varchar(8000) '$.procedure_code7_time',col_tooth_number varchar(8000) '$.tooth_number',col_surface_quadrant varchar(8000) '$.surface_quadrant',col_surface_quadrant_type varchar(8000) '$.surface_quadrant_type',col_schedule varchar(8000) '$.schedule',col_treatment_class varchar(8000) '$.treatment_class',col_cancelled varchar(8000) '$.cancelled',col_procedure_codes varchar(8000) '$.procedure_codes'
                            );

WITH
    CTE
    AS
    
    (
        SELECT col_href,
            RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
        FROM @TableView
    )
                        DELETE FROM CTE WHERE RN > 1

MERGE appointments original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_appointment_sr_no] = modified.[col_appointment_sr_no],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_operatory] = modified.[col_operatory],original.[col_date] = modified.[col_date],original.[col_time] = modified.[col_time],original.[col_length] = modified.[col_length],original.[col_description] = modified.[col_description],original.[col_amount] = modified.[col_amount],original.[col_status] = modified.[col_status],original.[col_last_changed_date] = modified.[col_last_changed_date],original.[col_appointment_made_date] = modified.[col_appointment_made_date],original.[col_type] = modified.[col_type],original.[col_procedure_code1] = modified.[col_procedure_code1],original.[col_procedure_code1_amount] = modified.[col_procedure_code1_amount],original.[col_procedure_code2] = modified.[col_procedure_code2],original.[col_procedure_code2_amount] = modified.[col_procedure_code2_amount],original.[col_procedure_code3] = modified.[col_procedure_code3],original.[col_procedure_code3_amount] = modified.[col_procedure_code3_amount],original.[col_procedure_code4] = modified.[col_procedure_code4],original.[col_procedure_code4_amount] = modified.[col_procedure_code4_amount],original.[col_procedure_code5] = modified.[col_procedure_code5],original.[col_procedure_code5_amount] = modified.[col_procedure_code5_amount],original.[col_procedure_code6] = modified.[col_procedure_code6],original.[col_procedure_code6_amount] = modified.[col_procedure_code6_amount],original.[col_procedure_code7] = modified.[col_procedure_code7],original.[col_procedure_code7_amount] = modified.[col_procedure_code7_amount],original.[col_hygienist] = modified.[col_hygienist],original.[col_provider_id] = modified.[col_provider_id],original.[col_provider] = modified.[col_provider],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_is_companion] = modified.[col_is_companion],original.[col_sooner_if_possible] = modified.[col_sooner_if_possible],original.[col_confirmed_on_date] = modified.[col_confirmed_on_date],original.[col_note] = modified.[col_note],original.[col_patient_name] = modified.[col_patient_name],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_guarantor_name] = modified.[col_guarantor_name],original.[col_procedure_code1_time] = modified.[col_procedure_code1_time],original.[col_procedure_code2_time] = modified.[col_procedure_code2_time],original.[col_procedure_code3_time] = modified.[col_procedure_code3_time],original.[col_procedure_code4_time] = modified.[col_procedure_code4_time],original.[col_procedure_code5_time] = modified.[col_procedure_code5_time],original.[col_procedure_code6_time] = modified.[col_procedure_code6_time],original.[col_procedure_code7_time] = modified.[col_procedure_code7_time],original.[col_tooth_number] = modified.[col_tooth_number],original.[col_surface_quadrant] = modified.[col_surface_quadrant],original.[col_surface_quadrant_type] = modified.[col_surface_quadrant_type],original.[col_schedule] = modified.[col_schedule],original.[col_treatment_class] = modified.[col_treatment_class],original.[col_cancelled] = modified.[col_cancelled],original.[col_procedure_codes] = modified.[col_procedure_codes],
                        original.updated_at = '2020-05-12T01:02:40.975Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_appointment_sr_no],[col_patient_id],[col_patient],[col_operatory],[col_date],[col_time],[col_length],[col_description],[col_amount],[col_status],[col_last_changed_date],[col_appointment_made_date],[col_type],[col_procedure_code1],[col_procedure_code1_amount],[col_procedure_code2],[col_procedure_code2_amount],[col_procedure_code3],[col_procedure_code3_amount],[col_procedure_code4],[col_procedure_code4_amount],[col_procedure_code5],[col_procedure_code5_amount],[col_procedure_code6],[col_procedure_code6_amount],[col_procedure_code7],[col_procedure_code7_amount],[col_hygienist],[col_provider_id],[col_provider],[col_practice_id],[col_practice],[col_is_companion],[col_sooner_if_possible],[col_confirmed_on_date],[col_note],[col_patient_name],[col_guarantor_id],[col_guarantor_name],[col_procedure_code1_time],[col_procedure_code2_time],[col_procedure_code3_time],[col_procedure_code4_time],[col_procedure_code5_time],[col_procedure_code6_time],[col_procedure_code7_time],[col_tooth_number],[col_surface_quadrant],[col_surface_quadrant_type],[col_schedule],[col_treatment_class],[col_cancelled],[col_procedure_codes],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_appointment_sr_no],modified.[col_patient_id],modified.[col_patient],modified.[col_operatory],modified.[col_date],modified.[col_time],modified.[col_length],modified.[col_description],modified.[col_amount],modified.[col_status],modified.[col_last_changed_date],modified.[col_appointment_made_date],modified.[col_type],modified.[col_procedure_code1],modified.[col_procedure_code1_amount],modified.[col_procedure_code2],modified.[col_procedure_code2_amount],modified.[col_procedure_code3],modified.[col_procedure_code3_amount],modified.[col_procedure_code4],modified.[col_procedure_code4_amount],modified.[col_procedure_code5],modified.[col_procedure_code5_amount],modified.[col_procedure_code6],modified.[col_procedure_code6_amount],modified.[col_procedure_code7],modified.[col_procedure_code7_amount],modified.[col_hygienist],modified.[col_provider_id],modified.[col_provider],modified.[col_practice_id],modified.[col_practice],modified.[col_is_companion],modified.[col_sooner_if_possible],modified.[col_confirmed_on_date],modified.[col_note],modified.[col_patient_name],modified.[col_guarantor_id],modified.[col_guarantor_name],modified.[col_procedure_code1_time],modified.[col_procedure_code2_time],modified.[col_procedure_code3_time],modified.[col_procedure_code4_time],modified.[col_procedure_code5_time],modified.[col_procedure_code6_time],modified.[col_procedure_code7_time],modified.[col_tooth_number],modified.[col_surface_quadrant],modified.[col_surface_quadrant_type],modified.[col_schedule],modified.[col_treatment_class],modified.[col_cancelled],modified.[col_procedure_codes],
                        '2020-05-12T01:02:40.975Z',
                        '2020-05-12T01:02:40.975Z',
                        'D18336',
                        'west cary dental'
                        );
                    
                       