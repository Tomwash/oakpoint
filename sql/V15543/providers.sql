
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/V15543/providers.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_provider_id varchar(8000),col_firstname varchar(8000),col_lastname varchar(8000),col_address_line1 varchar(8000),col_address_line2 varchar(8000),col_city varchar(8000),col_state varchar(8000),col_zipcode varchar(8000),col_country varchar(8000),col_phone varchar(8000),col_provider_type varchar(8000),col_specialty varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_patients varchar(8000),col_appointments varchar(8000),col_status varchar(8000),col_provider_account_id varchar(8000),col_state_id varchar(8000),col_tax_identification_number varchar(8000),col_medicaid_id varchar(8000),col_drug_id varchar(8000),col_national_provider_identifier varchar(8000),col_email varchar(8000),col_birthdate varchar(8000),col_hire_date varchar(8000),col_graduated_date varchar(8000),col_initials varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_provider_id],[col_firstname],[col_lastname],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_country],[col_phone],[col_provider_type],[col_specialty],[col_practice_id],[col_practice],[col_patients],[col_appointments],[col_status],[col_provider_account_id],[col_state_id],[col_tax_identification_number],[col_medicaid_id],[col_drug_id],[col_national_provider_identifier],[col_email],[col_birthdate],[col_hire_date],[col_graduated_date],[col_initials],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-21T02:22:53.922Z',
                            '2020-05-21T02:22:53.922Z',
                            'V15543',
                            '' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_provider_id varchar(8000) '$.provider_id',col_firstname varchar(8000) '$.firstname',col_lastname varchar(8000) '$.lastname',col_address_line1 varchar(8000) '$.address_line1',col_address_line2 varchar(8000) '$.address_line2',col_city varchar(8000) '$.city',col_state varchar(8000) '$.state',col_zipcode varchar(8000) '$.zipcode',col_country varchar(8000) '$.country',col_phone varchar(8000) '$.phone',col_provider_type varchar(8000) '$.provider_type',col_specialty varchar(8000) '$.specialty',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_patients varchar(8000) '$.patients',col_appointments varchar(8000) '$.appointments',col_status varchar(8000) '$.status',col_provider_account_id varchar(8000) '$.provider_account_id',col_state_id varchar(8000) '$.state_id',col_tax_identification_number varchar(8000) '$.tax_identification_number',col_medicaid_id varchar(8000) '$.medicaid_id',col_drug_id varchar(8000) '$.drug_id',col_national_provider_identifier varchar(8000) '$.national_provider_identifier',col_email varchar(8000) '$.email',col_birthdate varchar(8000) '$.birthdate',col_hire_date varchar(8000) '$.hire_date',col_graduated_date varchar(8000) '$.graduated_date',col_initials varchar(8000) '$.initials'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE providers original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_provider_id] = modified.[col_provider_id],original.[col_firstname] = modified.[col_firstname],original.[col_lastname] = modified.[col_lastname],original.[col_address_line1] = modified.[col_address_line1],original.[col_address_line2] = modified.[col_address_line2],original.[col_city] = modified.[col_city],original.[col_state] = modified.[col_state],original.[col_zipcode] = modified.[col_zipcode],original.[col_country] = modified.[col_country],original.[col_phone] = modified.[col_phone],original.[col_provider_type] = modified.[col_provider_type],original.[col_specialty] = modified.[col_specialty],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_patients] = modified.[col_patients],original.[col_appointments] = modified.[col_appointments],original.[col_status] = modified.[col_status],original.[col_provider_account_id] = modified.[col_provider_account_id],original.[col_state_id] = modified.[col_state_id],original.[col_tax_identification_number] = modified.[col_tax_identification_number],original.[col_medicaid_id] = modified.[col_medicaid_id],original.[col_drug_id] = modified.[col_drug_id],original.[col_national_provider_identifier] = modified.[col_national_provider_identifier],original.[col_email] = modified.[col_email],original.[col_birthdate] = modified.[col_birthdate],original.[col_hire_date] = modified.[col_hire_date],original.[col_graduated_date] = modified.[col_graduated_date],original.[col_initials] = modified.[col_initials],
                        original.updated_at = '2020-05-21T02:22:53.922Z',
                        original.office_id = 'V15543',
                        practice_name = ''
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_provider_id],[col_firstname],[col_lastname],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_country],[col_phone],[col_provider_type],[col_specialty],[col_practice_id],[col_practice],[col_patients],[col_appointments],[col_status],[col_provider_account_id],[col_state_id],[col_tax_identification_number],[col_medicaid_id],[col_drug_id],[col_national_provider_identifier],[col_email],[col_birthdate],[col_hire_date],[col_graduated_date],[col_initials],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_provider_id],modified.[col_firstname],modified.[col_lastname],modified.[col_address_line1],modified.[col_address_line2],modified.[col_city],modified.[col_state],modified.[col_zipcode],modified.[col_country],modified.[col_phone],modified.[col_provider_type],modified.[col_specialty],modified.[col_practice_id],modified.[col_practice],modified.[col_patients],modified.[col_appointments],modified.[col_status],modified.[col_provider_account_id],modified.[col_state_id],modified.[col_tax_identification_number],modified.[col_medicaid_id],modified.[col_drug_id],modified.[col_national_provider_identifier],modified.[col_email],modified.[col_birthdate],modified.[col_hire_date],modified.[col_graduated_date],modified.[col_initials],
                        '2020-05-21T02:22:53.922Z',
                        '2020-05-21T02:22:53.922Z',
                        'V15543',
                        ''
                        );
                    