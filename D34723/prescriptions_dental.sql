
                        -- Declare JSON Variable
                        declare @json nvarchar(max) = 
                        (
                            SELECT
                                CAST(BulkColumn AS NVARCHAR(MAX)) AS JsonData 
                            FROM 
                            OPENROWSET(BULK 'oakpoint-data/D34723/prescriptions_dental.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                        ); 

                        -- Declare Temp Table
                        Declare @TableView TABLE 
                        ( 
                            ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                            col_href varchar(8000),col_prescription_id varchar(8000),col_patient_id varchar(8000),col_patient varchar(8000),col_provider_id varchar(8000),col_provider varchar(8000),col_practice_id varchar(8000),col_practice varchar(8000),col_drug_id varchar(8000),col_drug varchar(8000),col_date varchar(8000),col_quantity varchar(8000),col_refills varchar(8000),col_direction varchar(8000),col_days varchar(8000),col_notes varchar(8000),created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000)
                        ); 

                        INSERT INTO @TableView
                            ([col_href],[col_prescription_id],[col_patient_id],[col_patient],[col_provider_id],[col_provider],[col_practice_id],[col_practice],[col_drug_id],[col_drug],[col_date],[col_quantity],[col_refills],[col_direction],[col_days],[col_notes],created_at, updated_at, office_id, practice_name)
                            SELECT *,
                            '2020-05-12T01:08:00.370Z',
                            '2020-05-12T01:08:00.370Z',
                            'D34723',
                            'Carolina Smiles' FROM OPENJSON(@json)  
                            WITH  
                            (
                                    col_href varchar(8000) '$.href',col_prescription_id varchar(8000) '$.prescription_id',col_patient_id varchar(8000) '$.patient_id',col_patient varchar(8000) '$.patient',col_provider_id varchar(8000) '$.provider_id',col_provider varchar(8000) '$.provider',col_practice_id varchar(8000) '$.practice_id',col_practice varchar(8000) '$.practice',col_drug_id varchar(8000) '$.drug_id',col_drug varchar(8000) '$.drug',col_date varchar(8000) '$.date',col_quantity varchar(8000) '$.quantity',col_refills varchar(8000) '$.refills',col_direction varchar(8000) '$.direction',col_days varchar(8000) '$.days',col_notes varchar(8000) '$.notes'
                            );

                        WITH CTE AS(
                            SELECT col_href,
                                RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                            FROM @TableView
                        )
                        DELETE FROM CTE WHERE RN > 1
                         
                        MERGE prescriptions_dental original
                        USING @TableView modified
                        ON original.col_href = modified.col_href
                        WHEN MATCHED 
                            THEN UPDATE SET 
                            original.[col_href] = modified.[col_href],original.[col_prescription_id] = modified.[col_prescription_id],original.[col_patient_id] = modified.[col_patient_id],original.[col_patient] = modified.[col_patient],original.[col_provider_id] = modified.[col_provider_id],original.[col_provider] = modified.[col_provider],original.[col_practice_id] = modified.[col_practice_id],original.[col_practice] = modified.[col_practice],original.[col_drug_id] = modified.[col_drug_id],original.[col_drug] = modified.[col_drug],original.[col_date] = modified.[col_date],original.[col_quantity] = modified.[col_quantity],original.[col_refills] = modified.[col_refills],original.[col_direction] = modified.[col_direction],original.[col_days] = modified.[col_days],original.[col_notes] = modified.[col_notes],
                        original.updated_at = '2020-05-12T01:08:00.370Z',
                        original.office_id = 'D34723',
                        practice_name = 'Carolina Smiles'
                        WHEN NOT MATCHED BY TARGET THEN
                        INSERT  
                        (
                            [col_href],[col_prescription_id],[col_patient_id],[col_patient],[col_provider_id],[col_provider],[col_practice_id],[col_practice],[col_drug_id],[col_drug],[col_date],[col_quantity],[col_refills],[col_direction],[col_days],[col_notes],created_at, updated_at, office_id, practice_name
                        )
                        VALUES
                        (
                            modified.[col_href],modified.[col_prescription_id],modified.[col_patient_id],modified.[col_patient],modified.[col_provider_id],modified.[col_provider],modified.[col_practice_id],modified.[col_practice],modified.[col_drug_id],modified.[col_drug],modified.[col_date],modified.[col_quantity],modified.[col_refills],modified.[col_direction],modified.[col_days],modified.[col_notes],
                        '2020-05-12T01:08:00.370Z',
                        '2020-05-12T01:08:00.370Z',
                        'D34723',
                        'Carolina Smiles'
                        );
                    