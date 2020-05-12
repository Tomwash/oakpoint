
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D24510/payment_plans.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_agreement_id varchar(8000),col_agreemen_type varchar(8000),col_guarantor_id varchar(8000),col_guarantor varchar(8000),col_patient_id varchar(8000),col_patient varchar(8000),col_payment_amount varchar(8000),col_number_of_payments varchar(8000),col_note varchar(8000),col_paymen_interval varchar(8000),col_create_date varchar(8000),col_startdate varchar(8000),col_enddate varchar(8000),col_interest_rate varchar(8000),col_loan_amount varchar(8000),col_remaining_balance varchar(8000),col_down_payment varchar(8000),col_finance_charges varchar(8000),col_late_charges varchar(8000),col_grace_period varchar(8000),col_minimum_balance_to_charge varchar(8000),col_description varchar(8000),col_is_closed varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_agreement_id],[col_agreemen_type],[col_guarantor_id],[col_guarantor],[col_patient_id],[col_patient],[col_payment_amount],[col_number_of_payments],[col_note],[col_paymen_interval],[col_create_date],[col_startdate],[col_enddate],[col_interest_rate],[col_loan_amount],[col_remaining_balance],[col_down_payment],[col_finance_charges],[col_late_charges],[col_grace_period],[col_minimum_balance_to_charge],[col_description],[col_is_closed],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:06:18.096Z',
                            '2020-05-12T01:06:18.096Z',
                            'D24510',
                            'Advanced Dental Center of Florence' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_agreement_id varchar(8000) '$.agreement_id',col_agreemen_type varchar(8000) '$.agreemen_type',col_guarantor_id varchar(8000) '$.guarantor_id',col_guarantor varchar(8000) '$.guarantor',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_payment_amount varchar(8000) '$.payment_amount',col_number_of_payments varchar(8000) '$.number_of_payments',col_note varchar(8000) '$.note',col_paymen_interval varchar(8000) '$.paymen_interval',col_create_date varchar(8000) '$.create_date',col_startdate varchar(8000) '$.startdate',col_enddate varchar(8000) '$.enddate',col_interest_rate varchar(8000) '$.interest_rate',col_loan_amount varchar(8000) '$.loan_amount',col_remaining_balance varchar(8000) '$.remaining_balance',col_down_payment varchar(8000) '$.down_payment',col_finance_charges varchar(8000) '$.finance_charges',col_late_charges varchar(8000) '$.late_charges',col_grace_period varchar(8000) '$.grace_period',col_minimum_balance_to_charge varchar(8000) '$.minimum_balance_to_charge',col_description varchar(8000) '$.description',col_is_closed varchar(8000) '$.is_closed',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE payment_plans original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_agreement_id] = modified.[col_agreement_id],original.[col_agreemen_type] = modified.[col_agreemen_type],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_guarantor] = modified.[col_guarantor],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_payment_amount] = modified.[col_payment_amount],original.[col_number_of_payments] = modified.[col_number_of_payments],original.[col_note] = modified.[col_note],original.[col_paymen_interval] = modified.[col_paymen_interval],original.[col_create_date] = modified.[col_create_date],original.[col_startdate] = modified.[col_startdate],original.[col_enddate] = modified.[col_enddate],original.[col_interest_rate] = modified.[col_interest_rate],original.[col_loan_amount] = modified.[col_loan_amount],original.[col_remaining_balance] = modified.[col_remaining_balance],original.[col_down_payment] = modified.[col_down_payment],original.[col_finance_charges] = modified.[col_finance_charges],original.[col_late_charges] = modified.[col_late_charges],original.[col_grace_period] = modified.[col_grace_period],original.[col_minimum_balance_to_charge] = modified.[col_minimum_balance_to_charge],original.[col_description] = modified.[col_description],original.[col_is_closed] = modified.[col_is_closed],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-12T01:06:18.096Z',
                        original.office_id = 'D24510',
                        practice_name = 'Advanced Dental Center of Florence'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_agreement_id],[col_agreemen_type],[col_guarantor_id],[col_guarantor],[col_patient_id],[col_patient],[col_payment_amount],[col_number_of_payments],[col_note],[col_paymen_interval],[col_create_date],[col_startdate],[col_enddate],[col_interest_rate],[col_loan_amount],[col_remaining_balance],[col_down_payment],[col_finance_charges],[col_late_charges],[col_grace_period],[col_minimum_balance_to_charge],[col_description],[col_is_closed],[col_practice_id],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_agreement_id],modified.[col_agreemen_type],modified.[col_guarantor_id],modified.[col_guarantor],modified.[col_patient_id],modified.[col_patient],modified.[col_payment_amount],modified.[col_number_of_payments],modified.[col_note],modified.[col_paymen_interval],modified.[col_create_date],modified.[col_startdate],modified.[col_enddate],modified.[col_interest_rate],modified.[col_loan_amount],modified.[col_remaining_balance],modified.[col_down_payment],modified.[col_finance_charges],modified.[col_late_charges],modified.[col_grace_period],modified.[col_minimum_balance_to_charge],modified.[col_description],modified.[col_is_closed],modified.[col_practice_id],modified.[col_practice],
                        '2020-05-12T01:06:18.096Z',
                        '2020-05-12T01:06:18.096Z',
                        'D24510',
                        'Advanced Dental Center of Florence'
                        );
                    