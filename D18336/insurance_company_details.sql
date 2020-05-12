
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D18336/insurance_company_details.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_insurance_company_id varchar(8000),col_insurance_company_name varchar(8000),col_address_line1 varchar(8000),col_address_line2 varchar(8000),col_beeper varchar(8000),col_cell varchar(8000),col_city varchar(8000),col_contact varchar(8000),col_default_plan varchar(8000),col_email1 varchar(8000),col_email2 varchar(8000),col_era_capable varchar(8000),col_ext1 varchar(8000),col_ext2 varchar(8000),col_ext3 varchar(8000),col_fax1 varchar(8000),col_fax2 varchar(8000),col_notes varchar(8000),col_payer_id varchar(8000),col_payer_type varchar(8000),col_phone1 varchar(8000),col_phone2 varchar(8000),col_phone3 varchar(8000),col_practice_id varchar(8000),col_provider_practice_id varchar(8000),col_state varchar(8000),col_trojan_id varchar(8000),col_web_link varchar(8000),col_zipcode varchar(8000),col_practice varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_insurance_company_id],[col_insurance_company_name],[col_address_line1],[col_address_line2],[col_beeper],[col_cell],[col_city],[col_contact],[col_default_plan],[col_email1],[col_email2],[col_era_capable],[col_ext1],[col_ext2],[col_ext3],[col_fax1],[col_fax2],[col_notes],[col_payer_id],[col_payer_type],[col_phone1],[col_phone2],[col_phone3],[col_practice_id],[col_provider_practice_id],[col_state],[col_trojan_id],[col_web_link],[col_zipcode],[col_practice],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:04:08.184Z',
                            '2020-05-12T01:04:08.184Z',
                            'D18336',
                            'west cary dental' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_insurance_company_id varchar(8000) '$.insurance_company_id',col_insurance_company_name varchar(8000) '$.insurance_company_name',col_address_line1 varchar(8000) '$.address_line1',col_address_line2 varchar(8000) '$.address_line2',col_beeper varchar(8000) '$.beeper',col_cell varchar(8000) '$.cell',col_city varchar(8000) '$.city',col_contact varchar(8000) '$.contact',col_default_plan varchar(8000) '$.default_plan',col_email1 varchar(8000) '$.email1',col_email2 varchar(8000) '$.email2',col_era_capable varchar(8000) '$.era_capable',col_ext1 varchar(8000) '$.ext1',col_ext2 varchar(8000) '$.ext2',col_ext3 varchar(8000) '$.ext3',col_fax1 varchar(8000) '$.fax1',col_fax2 varchar(8000) '$.fax2',col_notes varchar(8000) '$.notes',col_payer_id varchar(8000) '$.payer_id',col_payer_type varchar(8000) '$.payer_type',col_phone1 varchar(8000) '$.phone1',col_phone2 varchar(8000) '$.phone2',col_phone3 varchar(8000) '$.phone3',col_practice_id varchar(8000) '$.practice_id',col_provider_practice_id varchar(8000) '$.provider_practice_id',col_state varchar(8000) '$.state',col_trojan_id varchar(8000) '$.trojan_id',col_web_link varchar(8000) '$.web_link',col_zipcode varchar(8000) '$.zipcode',col_practice varchar(8000) '$.practice'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE insurance_company_details original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_insurance_company_id] = modified.[col_insurance_company_id],original.[col_insurance_company_name] = modified.[col_insurance_company_name],original.[col_address_line1] = modified.[col_address_line1],original.[col_address_line2] = modified.[col_address_line2],original.[col_beeper] = modified.[col_beeper],original.[col_cell] = modified.[col_cell],original.[col_city] = modified.[col_city],original.[col_contact] = modified.[col_contact],original.[col_default_plan] = modified.[col_default_plan],original.[col_email1] = modified.[col_email1],original.[col_email2] = modified.[col_email2],original.[col_era_capable] = modified.[col_era_capable],original.[col_ext1] = modified.[col_ext1],original.[col_ext2] = modified.[col_ext2],original.[col_ext3] = modified.[col_ext3],original.[col_fax1] = modified.[col_fax1],original.[col_fax2] = modified.[col_fax2],original.[col_notes] = modified.[col_notes],original.[col_payer_id] = modified.[col_payer_id],original.[col_payer_type] = modified.[col_payer_type],original.[col_phone1] = modified.[col_phone1],original.[col_phone2] = modified.[col_phone2],original.[col_phone3] = modified.[col_phone3],original.[col_practice_id] = modified.[col_practice_id],original.[col_provider_practice_id] = modified.[col_provider_practice_id],original.[col_state] = modified.[col_state],original.[col_trojan_id] = modified.[col_trojan_id],original.[col_web_link] = modified.[col_web_link],original.[col_zipcode] = modified.[col_zipcode],original.[col_practice] = modified.[col_practice],
                        original.updated_at = '2020-05-12T01:04:08.184Z',
                        original.office_id = 'D18336',
                        practice_name = 'west cary dental'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_insurance_company_id],[col_insurance_company_name],[col_address_line1],[col_address_line2],[col_beeper],[col_cell],[col_city],[col_contact],[col_default_plan],[col_email1],[col_email2],[col_era_capable],[col_ext1],[col_ext2],[col_ext3],[col_fax1],[col_fax2],[col_notes],[col_payer_id],[col_payer_type],[col_phone1],[col_phone2],[col_phone3],[col_practice_id],[col_provider_practice_id],[col_state],[col_trojan_id],[col_web_link],[col_zipcode],[col_practice],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_insurance_company_id],modified.[col_insurance_company_name],modified.[col_address_line1],modified.[col_address_line2],modified.[col_beeper],modified.[col_cell],modified.[col_city],modified.[col_contact],modified.[col_default_plan],modified.[col_email1],modified.[col_email2],modified.[col_era_capable],modified.[col_ext1],modified.[col_ext2],modified.[col_ext3],modified.[col_fax1],modified.[col_fax2],modified.[col_notes],modified.[col_payer_id],modified.[col_payer_type],modified.[col_phone1],modified.[col_phone2],modified.[col_phone3],modified.[col_practice_id],modified.[col_provider_practice_id],modified.[col_state],modified.[col_trojan_id],modified.[col_web_link],modified.[col_zipcode],modified.[col_practice],
                        '2020-05-12T01:04:08.184Z',
                        '2020-05-12T01:04:08.184Z',
                        'D18336',
                        'west cary dental'
                        );
                    