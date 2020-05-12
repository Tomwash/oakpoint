
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/V15543/guarantors_v2.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_guarantor_id varchar(8000),col_firstname varchar(8000),col_middlename varchar(8000),col_lastname varchar(8000),col_preferred_name varchar(8000),col_salutation varchar(8000),col_birthdate varchar(8000),col_employer_id varchar(8000),col_address_line1 varchar(8000),col_address_line2 varchar(8000),col_city varchar(8000),col_state varchar(8000),col_zipcode varchar(8000),col_phone varchar(8000),col_workphone varchar(8000),col_cell varchar(8000),col_guarantor_class varchar(8000),col_email varchar(8000),col_preferred_contact varchar(8000),col_billing_type varchar(8000),col_status varchar(8000),col_suspended_date varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_employer varchar(8000),col_country varchar(8000),col_client_type_code varchar(8000),col_client_class varchar(8000),col_ssn_on_file varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_guarantor_id],[col_firstname],[col_middlename],[col_lastname],[col_preferred_name],[col_salutation],[col_birthdate],[col_employer_id],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_phone],[col_workphone],[col_cell],[col_guarantor_class],[col_email],[col_preferred_contact],[col_billing_type],[col_status],[col_suspended_date],[col_practice_id],[col_practice],[col_employer],[col_country],[col_client_type_code],[col_client_class],[col_ssn_on_file],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:09:03.502Z',
                            '2020-05-12T01:09:03.502Z',
                            'V15543',
                            '' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_guarantor_id varchar(8000) '$.guarantor_id',col_firstname varchar(8000) '$.firstname',col_middlename varchar(8000) '$.middlename',col_lastname varchar(8000) '$.lastname',col_preferred_name varchar(8000) '$.preferred_name',col_salutation varchar(8000) '$.salutation',col_birthdate varchar(8000) '$.birthdate',col_employer_id varchar(8000) '$.employer_id',col_address_line1 varchar(8000) '$.address_line1',col_address_line2 varchar(8000) '$.address_line2',col_city varchar(8000) '$.city',col_state varchar(8000) '$.state',col_zipcode varchar(8000) '$.zipcode',col_phone varchar(8000) '$.phone',col_workphone varchar(8000) '$.workphone',col_cell varchar(8000) '$.cell',col_guarantor_class varchar(8000) '$.guarantor_class',col_email varchar(8000) '$.email',col_preferred_contact varchar(8000) '$.preferred_contact',col_billing_type varchar(8000) '$.billing_type',col_status varchar(8000) '$.status',col_suspended_date varchar(8000) '$.suspended_date',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_employer varchar(8000) '$.employer',col_country varchar(8000) '$.country',col_client_type_code varchar(8000) '$.client_type_code',col_client_class varchar(8000) '$.client_class',col_ssn_on_file varchar(8000) '$.ssn_on_file'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE guarantors_v2 original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_guarantor_id] = modified.[col_guarantor_id],original.[col_firstname] = modified.[col_firstname],original.[col_middlename] = modified.[col_middlename],original.[col_lastname] = modified.[col_lastname],original.[col_preferred_name] = modified.[col_preferred_name],original.[col_salutation] = modified.[col_salutation],original.[col_birthdate] = modified.[col_birthdate],original.[col_employer_id] = modified.[col_employer_id],original.[col_address_line1] = modified.[col_address_line1],original.[col_address_line2] = modified.[col_address_line2],original.[col_city] = modified.[col_city],original.[col_state] = modified.[col_state],original.[col_zipcode] = modified.[col_zipcode],original.[col_phone] = modified.[col_phone],original.[col_workphone] = modified.[col_workphone],original.[col_cell] = modified.[col_cell],original.[col_guarantor_class] = modified.[col_guarantor_class],original.[col_email] = modified.[col_email],original.[col_preferred_contact] = modified.[col_preferred_contact],original.[col_billing_type] = modified.[col_billing_type],original.[col_status] = modified.[col_status],original.[col_suspended_date] = modified.[col_suspended_date],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_employer] = modified.[col_employer],original.[col_country] = modified.[col_country],original.[col_client_type_code] = modified.[col_client_type_code],original.[col_client_class] = modified.[col_client_class],original.[col_ssn_on_file] = modified.[col_ssn_on_file],
                        original.updated_at = '2020-05-12T01:09:03.502Z',
                        original.office_id = 'V15543',
                        practice_name = ''
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_guarantor_id],[col_firstname],[col_middlename],[col_lastname],[col_preferred_name],[col_salutation],[col_birthdate],[col_employer_id],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_phone],[col_workphone],[col_cell],[col_guarantor_class],[col_email],[col_preferred_contact],[col_billing_type],[col_status],[col_suspended_date],[col_practice_id],[col_practice],[col_employer],[col_country],[col_client_type_code],[col_client_class],[col_ssn_on_file],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_guarantor_id],modified.[col_firstname],modified.[col_middlename],modified.[col_lastname],modified.[col_preferred_name],modified.[col_salutation],modified.[col_birthdate],modified.[col_employer_id],modified.[col_address_line1],modified.[col_address_line2],modified.[col_city],modified.[col_state],modified.[col_zipcode],modified.[col_phone],modified.[col_workphone],modified.[col_cell],modified.[col_guarantor_class],modified.[col_email],modified.[col_preferred_contact],modified.[col_billing_type],modified.[col_status],modified.[col_suspended_date],modified.[col_practice_id],modified.[col_practice],modified.[col_employer],modified.[col_country],modified.[col_client_type_code],modified.[col_client_class],modified.[col_ssn_on_file],
                        '2020-05-12T01:09:03.502Z',
                        '2020-05-12T01:09:03.502Z',
                        'V15543',
                        ''
                        );
                    