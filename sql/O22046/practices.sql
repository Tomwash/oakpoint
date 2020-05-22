
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                               '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/streams/O22046/practices.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_practice_id varchar(8000),col_name varchar(8000),col_address_line1 varchar(8000),col_address_line2 varchar(8000),col_city varchar(8000),col_state varchar(8000),col_zipcode varchar(8000),col_phone varchar(8000),col_country varchar(8000),col_email varchar(8000),col_website varchar(8000),col_description varchar(8000),col_providers varchar(8000),col_patients varchar(8000),col_guarantors varchar(8000),col_appointments varchar(8000),col_transactions varchar(8000),col_cust_id varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_practice_id],[col_name],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_phone],[col_country],[col_email],[col_website],[col_description],[col_providers],[col_patients],[col_guarantors],[col_appointments],[col_transactions],[col_cust_id],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-21T02:21:35.547Z',
                            '2020-05-21T02:21:35.547Z',
                            'O22046',
                            'Carolina Braces' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_practice_id varchar(8000) '$.practice_id',col_name varchar(8000) '$.name',col_address_line1 varchar(8000) '$.address_line1',col_address_line2 varchar(8000) '$.address_line2',col_city varchar(8000) '$.city',col_state varchar(8000) '$.state',col_zipcode varchar(8000) '$.zipcode',col_phone varchar(8000) '$.phone',col_country varchar(8000) '$.country',col_email varchar(8000) '$.email',col_website varchar(8000) '$.website',col_description varchar(8000) '$.description',col_providers varchar(8000) '$.providers',col_patients varchar(8000) '$.patients',col_guarantors varchar(8000) '$.guarantors',col_appointments varchar(8000) '$.appointments',col_transactions varchar(8000) '$.transactions',col_cust_id varchar(8000) '$.cust_id'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE practices original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_practice_id] = modified.[col_practice_id],original.[col_name] = modified.[col_name],original.[col_address_line1] = modified.[col_address_line1],original.[col_address_line2] = modified.[col_address_line2],original.[col_city] = modified.[col_city],original.[col_state] = modified.[col_state],original.[col_zipcode] = modified.[col_zipcode],original.[col_phone] = modified.[col_phone],original.[col_country] = modified.[col_country],original.[col_email] = modified.[col_email],original.[col_website] = modified.[col_website],original.[col_description] = modified.[col_description],original.[col_providers] = modified.[col_providers],original.[col_patients] = modified.[col_patients],original.[col_guarantors] = modified.[col_guarantors],original.[col_appointments] = modified.[col_appointments],original.[col_transactions] = modified.[col_transactions],original.[col_cust_id] = modified.[col_cust_id],
                        original.updated_at = '2020-05-21T02:21:35.547Z',
                        original.office_id = 'O22046',
                        practice_name = 'Carolina Braces'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_practice_id],[col_name],[col_address_line1],[col_address_line2],[col_city],[col_state],[col_zipcode],[col_phone],[col_country],[col_email],[col_website],[col_description],[col_providers],[col_patients],[col_guarantors],[col_appointments],[col_transactions],[col_cust_id],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_practice_id],modified.[col_name],modified.[col_address_line1],modified.[col_address_line2],modified.[col_city],modified.[col_state],modified.[col_zipcode],modified.[col_phone],modified.[col_country],modified.[col_email],modified.[col_website],modified.[col_description],modified.[col_providers],modified.[col_patients],modified.[col_guarantors],modified.[col_appointments],modified.[col_transactions],modified.[col_cust_id],
                        '2020-05-21T02:21:35.547Z',
                        '2020-05-21T02:21:35.547Z',
                        'O22046',
                        'Carolina Braces'
                        );
                    