const fs = require('fs');
const sql = require("mssql");
const { BlobServiceClient } = require('@azure/storage-blob');

const { sikkaApi } = require('../SikkaApi');
const config = require('../config');

const {
    azureSqlConfig,
    azureStorageConfig: {
        connectionString,
        containerName
    }
} = config;

module.exports = async function (context, req) {

    context.log('JavaScript Authorized Practices function processed a request.');
    // Create the BlobServiceClient object which will be used to create a container client
    const blobServiceClient = await BlobServiceClient.fromConnectionString(connectionString);

    // // Get a reference to a container
    const containerClient = await blobServiceClient.getContainerClient(containerName);

    for await (const blob of containerClient.listBlobsFlat()) {
        console.log(blob.name);
    }


    // const pool = new sql.ConnectionPool(azureSqlConfig);
    // await pool.connect();

    // const authorizedPractices = await sikkaApi.authorizedPractices();

    // context.log(`Authorized Practices: ${authorizedPractices[0].total_count}`);
    // for (let i = 0; i < authorizedPractices[0].total_count; i += 1) {
    //     // loop through all practices

    //     const office = authorizedPractices[0].items[i];
    //     const { office_id, secret_key, practice_name } = office;

    //     if ([
    //         'D18336',
    //         'D22072',
    //         'D24510',
    //         // 'D34723',
    //         // 'O22046',
    //         // 'V15543'
    //     ].includes(office_id)) {
    //         context.log(`Skipping ${office_id}`);
    //         continue;
    //     }

    //     // get request key
    //     context.log(`Retrieving Request Key for Office ${office_id}`);
    //     const request_key = await sikkaApi.requestKey(office_id, secret_key);
    //     console.log(request_key);

    //     // get practice's request key info
    //     context.log(`Retrieving Request Key Info ${office_id}, Using The Request Key`);
    //     const requestKeyInfo = await sikkaApi.requestKeyInfo(request_key.request_key);
    //     const listOfAccessibleApis = requestKeyInfo.scope.split(',')

    //     // loop through each authorized endpoint for this practice and drop the data in sql
    //     // This is each table they are authorizde to use. At this point, execute merge query.
    //     const queries = listOfAccessibleApis.map(async (api) => {

    //         if (['start', 'end'].includes(api)) {
    //             return new Promise();
    //         }

    //         const resourceResponse = await sikkaApi.getSingleResourceByRequestKey(request_key.request_key, api);
    //         const tableName = api.replace('/ ', '__');

    //         // if (tableName === 'appointments') { context.log(resourceResponse); } else {
    //         //     continue;
    //         // }

    //         if (resourceResponse.href) {

    //             const currentDate = new Date();
    //             const currentIsoDateTime = currentDate.toISOString();

    //             context.log('Writing to SQL Database', tableName, office_id);

    //             let insertColumns = '';
    //             let columnsStringCreateTable = '';
    //             let updateQuery = '';
    //             let withOpenJson = '';
    //             let mergeQueryInsertValuesFromModified = '';
    //             Object.keys(resourceResponse).forEach(key => {
    //                 insertColumns += `[col_${key}],`;
    //                 columnsStringCreateTable += `col_${key} varchar(8000),`;
    //                 updateQuery += `original.[col_${key}] = modified.[col_${key}],`;
    //                 mergeQueryInsertValuesFromModified += `modified.[col_${key}],`;
    //                 withOpenJson += `col_${key} varchar(8000) '$.${key}',`;
    //             })

    //             // add metadata fields
    //             columnsStringCreateTable += 'created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000),'
    //             insertColumns += 'created_at, updated_at, office_id, practice_name,'
    //             updateQuery +=
    //                 `
    //                     original.updated_at = '${currentIsoDateTime}',
    //                     original.office_id = '${office_id}',
    //                     practice_name = '${practice_name.replace(/'/g, "''")}',`
    //             mergeQueryInsertValuesFromModified +=
    //                 `
    //                     '${currentIsoDateTime}',
    //                     '${currentIsoDateTime}',
    //                     '${office_id}',
    //                     '${practice_name.replace(/'/g, "''")}',`

    //             // slice off trailing comma
    //             columnsStringCreateTable = columnsStringCreateTable.slice(0, -1);
    //             insertColumns = insertColumns.slice(0, -1);
    //             updateQuery = updateQuery.slice(0, -1);
    //             mergeQueryInsertValuesFromModified = mergeQueryInsertValuesFromModified.slice(0, -1)
    //             withOpenJson = withOpenJson.slice(0, -1);

    //             const query =
    //                 `
    //                     -- Declare JSON Variable
    //                     declare @json nvarchar(max) = 
    //                     (
    //                         SELECT
    //                            '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
    //                         FROM 
    //                         OPENROWSET(BULK 'oakpoint-data/streams/${office_id}/${tableName}.json', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
    //                     ); 

    //                     -- Declare Temp Table
    //                     Declare @TableView TABLE 
    //                     ( 
    //                         ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
    //                         ${columnsStringCreateTable}
    //                     ); 

    //                     INSERT INTO @TableView
    //                         (${insertColumns})
    //                         SELECT *,
    //                         '${currentIsoDateTime}',
    //                         '${currentIsoDateTime}',
    //                         '${office_id}',
    //                         '${practice_name.replace(/'/g, "''")}' FROM OPENJSON(@json)  
    //                         WITH  
    //                         (
    //                                 ${withOpenJson}
    //                         );

    //                     WITH CTE AS(
    //                         SELECT col_href,
    //                             RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
    //                         FROM @TableView
    //                     )
    //                     DELETE FROM CTE WHERE RN > 1

    //                     MERGE ${tableName} original
    //                     USING @TableView modified
    //                     ON original.col_href = modified.col_href
    //                     WHEN MATCHED 
    //                         THEN UPDATE SET 
    //                         ${updateQuery}
    //                     WHEN NOT MATCHED BY TARGET THEN
    //                     INSERT  
    //                     (
    //                         ${insertColumns}
    //                     )
    //                     VALUES
    //                     (
    //                         ${mergeQueryInsertValuesFromModified}
    //                     );
    //                 `

    //             // if (tableName === 'appointments') { context.log(query); }
    //             try {
    //                 if (tableName) {
    //                     const tableCreateQuery = `
    //                     IF(object_id('${tableName}', 'U') is null) BEGIN
    //                     print 'Must create the table!';
    //                     CREATE TABLE ${tableName} (
    //                         ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
    //                         ${columnsStringCreateTable}
    //                     );

    //                     CREATE UNIQUE INDEX col_href
    //                     ON ${tableName} (col_href)

    //                     CREATE INDEX non_unique_office_metadata
    //                     ON ${tableName} (office_id, practice_name, created_at, updated_at)
    //                     END
    //                     `
    //                     // context.log(tableCreateQuery);
    //                     await pool.query(tableCreateQuery);

    //                     // Create file for office to store the JSON response and prep for upload
    //                     const fileLocation = `sql/${office_id}`;
    //                     const fileName = `${tableName}.sql`
    //                     const pathToFile = `${fileLocation}/${fileName}`

    //                     if (!fs.existsSync(fileLocation)) {
    //                         fs.mkdirSync(fileLocation);
    //                     }

    //                     fs.appendFileSync(pathToFile, query, (err) => {
    //                         if (err) {
    //                             context.log(err);
    //                         }
    //                         context.log('It\'s saved!');
    //                     });
    //                 } else {
    //                     throw new Error(tableName)
    //                 }
    //             }
    //             catch (err) {
    //                 context.log(err);
    //             }
    //             try {
    //                 await pool.query(query)
    //             } catch (err) {
    //                 console.log('Error inside of query execution')
    //             }
    //         } else {
    //             context.log('NO DATA')
    //         }
    //     });

    //     let responseFromAllPromises;
    //     try {
    //         responseFromAllPromises = await Promise.all(queries);
    //     } catch (err) {
    //         console.log('error in promises', err)
    //     }

    // }


    // await pool.close();

    context.log('Process Complete:');
    context.res = {
        status: 200,
        body: 'success'
    };
}