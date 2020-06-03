const fetch = require("node-fetch");
const queryString = require('query-string');
const getStream = require('into-stream');
const { v4: uuidV4 } = require('uuid');
const sql = require("mssql");

const {
    BlobServiceClient
} = require('@azure/storage-blob');

const config = require('../config');

const {
    azureSqlConfig,
    azureStorageConfig: {
        connectionString,
        containerName
    },
    sikkaApiConfig: {
        authUrl,
        baseUrl,
        version,
        app_id,
        app_key
    }
} = config;

async function get(url) {
    const response = await fetch(url);
    const json = await response.json();

    return json;
}

//SPC Support API
async function healthCheck(officeId) {
    const url = `${authUrl}${version}health_check/${officeId}`;
    return get(url);
}

// Authentication API
async function applications() {
    const url = `${authUrl}${version}applications?${queryString.stringify({ app_id, app_key })}`;
    return get(url);
}

async function authorizedPractices() {
    const url = `${authUrl}${version}authorized_practices?${queryString.stringify({ app_id, app_key })}`;
    return get(url);
}

async function authorizedPracticesByOfficeId(office_id) {
    const url = `${authUrl}${version}authorized_practices/${office_id}?${queryString.stringify({ app_id, app_key })}`;
    return get(url);
}

async function requestKeyInfo(request_key) {
    const url = `${baseUrl}${version}request_key_info?${queryString.stringify({ request_key })}`;
    return get(url);
}

async function requestKey(office_id, secret_key) {
    const url = `${authUrl}${version}request_keys?${queryString.stringify({ app_id, app_key, office_id, secret_key })}`;
    return get(url);
}

async function requestKeyDetails(request_key) {
    const url = `${authUrl}${version}request_keys_details/${request_key}?${queryString.stringify({ app_id, app_key })}`;
    return get(url);
}

async function dataCheck(request_key) {
    const url = `${baseUrl}${version}data_check?${queryString.stringify({ request_key })}`;
    return get(url);
}

async function getBaseResourceByRequestKeyAndDumpToBlob(request_key, resourceUri, blobLocation, metadata = {}) {
    var startDate = new Date();
    var endDate = new Date();
    var loaded_startdate;
    var loaded_enddate;

    startDate.setDate(startDate.getDate() - 5);
    endDate.setDate(endDate.getDate());

    loaded_startdate = `${startDate.getFullYear()}-${('0' + (startDate.getMonth() + 1)).slice(-2)}-${('0' + startDate.getDate()).slice(-2)}`

    loaded_enddate = `${endDate.getFullYear()}-${('0' + (endDate.getMonth() + 1)).slice(-2)}-${('0' + endDate.getDate()).slice(-2)}`
    let data;
    const url = `${baseUrl}${version}${resourceUri}?${queryString.stringify({ request_key, limit: 5000, offset: 0, loaded_startdate, loaded_enddate })}`;
    try {
        console.log("Fetching data")
        console.log(url);
        data = await get(url);
    } catch (err) {
        console.log("WARNING: ERROR IN MAIN REQUEST, PROBABLY A 204", data, err)
        return;
    }

    if (data[0]) {
        const items = data[0].items.map(item => JSON.stringify(item));
        console.log("Creating merge query")
        const listOfBlockIds = [];

        let blockId;

        blockId = Buffer.from(uuidV4()).toString('base64');

        console.log('writing stream in staged blocks before loop', items.length);
        let appendString = '';
        if (data[0].pagination) {
            if (data[0].pagination.next) {
                appendString = ',';
            }
        }
        await writeStreamInStagedBlocks(blockId, items.join(',') + appendString, blobLocation);
        listOfBlockIds.push(blockId);

        if (data[0].pagination) {
            while (data[0].pagination.next) {
                const nextUrl = `${data[0].pagination.next}&request_key=${request_key}`;
                try {
                    console.log('getting data', nextUrl);

                    data = await get(nextUrl);
                    if (data[0].items.length === 0) {
                        throw new Error('no items found')
                    }
                    const itemsInLoop = data[0].items.map(item => JSON.stringify(item));
                    console.log('writing stream in staged blocks inside loop', itemsInLoop.length);

                    blockId = Buffer.from(uuidV4()).toString('base64');
                    await writeStreamInStagedBlocks(blockId, itemsInLoop.join(',') + (data[0].pagination.next ? ',' : ''), blobLocation);
                    listOfBlockIds.push(blockId);
                } catch (err) {
                    console.log(err);
                }
            }
        }

        await commitStagedBlocks(listOfBlockIds, blobLocation);
        await createMergeQuery(data[0].items[0], metadata, blobLocation);
        console.log(`commit of blockid list successful:\n\n ${listOfBlockIds}\n\n\n commit block response \n\n`);

        return;
    }

    return;

}

async function getSingleResourceByRequestKey(request_key, resourceUri) {

    let data;
    const url = `${baseUrl}${version}${resourceUri}?${queryString.stringify({ request_key, limit: 1 })}`;
    try {
        data = await get(url);
    } catch (err) {
        console.log("WARNING: ERROR IN MAIN REQUEST, PROBABLY A 204", data, err)
        return [];
    }
    return data;

}


async function writeStreamInStagedBlocks(blockId, jsonString, blobLocation) {
    // Create the BlobServiceClient object which will be used to create a container client
    const blobServiceClient = await BlobServiceClient.fromConnectionString(connectionString);

    // // Get a reference to a container
    const containerClient = await blobServiceClient.getContainerClient(containerName);

    // Get blob block client, used modifying blobs in azure storage
    const blockBlobClient = await containerClient.getBlockBlobClient(blobLocation);

    const exists = await blockBlobClient.exists();
    if (!exists) {
        await containerClient
    }

    return await blockBlobClient.stageBlock(blockId, jsonString, jsonString.length | 0);
}

async function commitStagedBlocks(listOfBlockIds, blobLocation) {
    // Create the BlobServiceClient object which will be used to create a container client
    const blobServiceClient = await BlobServiceClient.fromConnectionString(connectionString);

    // // Get a reference to a container
    const containerClient = await blobServiceClient.getContainerClient(containerName);

    // Get blob block client, used modifying blobs in azure storage
    const blockBlobClient = await containerClient.getBlockBlobClient(blobLocation);

    return await blockBlobClient.commitBlockList(listOfBlockIds);
}

async function clearBlobs() {
    // Create the BlobServiceClient object which will be used to create a container client
    const blobServiceClient = await BlobServiceClient.fromConnectionString(connectionString);

    // // Get a reference to a container
    const containerClient = await blobServiceClient.getContainerClient(containerName);

    for await (const blob of containerClient.listBlobsFlat()) {
        if (blob.name.toString().includes('streams') || (blob.name.toString().includes('sql') && blob.name.toString().includes('recent'))) { continue; }
        await containerClient.deleteBlob(blob.name);
    }


}


async function createMergeQuery(resourceResponse, metadata, blobLocation) {

    const { office_id, tableName, practice_name } = metadata;

    const pool = new sql.ConnectionPool(azureSqlConfig);

    const fileLocation = `sql-recent`;
    const fileName = `${office_id}/${tableName}.sql`
    const pathToFile = `${fileLocation}/${fileName}`

    // Create the BlobServiceClient object which will be used to create a container client
    const blobServiceClient = await BlobServiceClient.fromConnectionString(connectionString);

    // // Get a reference to a container
    const containerClient = await blobServiceClient.getContainerClient(containerName);

    // Get blob block client, used modifying blobs in azure storage
    const blockBlobClient = await containerClient.getBlockBlobClient(pathToFile)

    console.log(resourceResponse.href)
    if (resourceResponse.href) {

        // ======================================== //
        // ======================================== //
        // =========== Setup query params ========= //
        // ======================================== //
        // ======================================== //
        const currentDate = new Date();
        const currentIsoDateTime = currentDate.toISOString();

        let insertColumns = '';
        let columnsStringCreateTable = '';
        let updateQuery = '';
        let withOpenJson = '';
        let mergeQueryInsertValuesFromModified = '';
        // const columnLength = '8000';
        Object.keys(resourceResponse).forEach(key => {
            insertColumns += `[col_${key}],`;
            columnsStringCreateTable += `col_${key} varchar(8000),`;
            updateQuery += `original.[col_${key}] = modified.[col_${key}],`;
            mergeQueryInsertValuesFromModified += `modified.[col_${key}],`;
            withOpenJson += `col_${key} varchar(8000) '$.${key}',`;
        })

        // add metadata fields
        columnsStringCreateTable += 'created_at datetime, updated_at datetime, office_id varchar(2000), practice_name varchar(2000),'
        insertColumns += 'created_at, updated_at, office_id, practice_name,'
        updateQuery +=
            `
                            original.updated_at = '${currentIsoDateTime}',
                            original.office_id = '${office_id}',
                            practice_name = '${practice_name.replace(/'/g, "''")}',`
        mergeQueryInsertValuesFromModified +=
            `
                            '${currentIsoDateTime}',
                            '${currentIsoDateTime}',
                            '${office_id}',
                            '${practice_name.replace(/'/g, "''")}',`

        // slice off trailing comma
        columnsStringCreateTable = columnsStringCreateTable.slice(0, -1);
        insertColumns = insertColumns.slice(0, -1);
        updateQuery = updateQuery.slice(0, -1);
        mergeQueryInsertValuesFromModified = mergeQueryInsertValuesFromModified.slice(0, -1)
        withOpenJson = withOpenJson.slice(0, -1);

        const query =
            `
                            -- Declare JSON Variable
                            declare @json nvarchar(max) = 
                            (
                                SELECT
                                   '[' + CAST(BulkColumn AS NVARCHAR(MAX)) + ']' AS JsonData 
                                FROM 
                                OPENROWSET(BULK 'oakpoint-data/${blobLocation}', DATA_SOURCE = 'OakpointDataV1', SINGLE_CLOB) AS AzureBlob 
                            ); 
    
                            -- Declare Temp Table
                            Declare @TableView TABLE 
                            ( 
                                ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                                ${columnsStringCreateTable}
                            ); 
    
                            INSERT INTO @TableView
                                (${insertColumns})
                                SELECT *,
                                '${currentIsoDateTime}',
                                '${currentIsoDateTime}',
                                '${office_id}',
                                '${practice_name.replace(/'/g, "''")}' FROM OPENJSON(@json)  
                                WITH  
                                (
                                        ${withOpenJson}
                                );
    
                            WITH CTE AS(
                                SELECT col_href,
                                    RN = ROW_NUMBER()OVER(PARTITION BY col_href ORDER BY col_href)
                                FROM @TableView
                            )
                            DELETE FROM CTE WHERE RN > 1
    
                            MERGE ${tableName} original
                            USING @TableView modified
                            ON original.col_href = modified.col_href
                            WHEN MATCHED 
                                THEN UPDATE SET 
                                ${updateQuery}
                            WHEN NOT MATCHED BY TARGET THEN
                            INSERT  
                            (
                                ${insertColumns}
                            )
                            VALUES
                            (
                                ${mergeQueryInsertValuesFromModified}
                            );
                        `
        // ======================================== //
        // ================ Execute =============== //
        // =========== Create Table Query ========= //
        // ======================================== //
        // ======================================== //
        try {
            if (tableName) {
                const tableCreateQuery = `
                            IF(object_id('${tableName}', 'U') is null) BEGIN
                            print 'Must create the table!';
                            CREATE TABLE ${tableName} (
                                ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                                ${columnsStringCreateTable}
                            );
    
                            CREATE UNIQUE INDEX col_href
                            ON ${tableName} (col_href)
    
                            CREATE INDEX non_unique_office_metadata
                            ON ${tableName} (office_id, practice_name, created_at, updated_at)
                            END
                            `
                await pool.connect();
                try {
                    await pool.query(tableCreateQuery);
                } catch (err) {
                    console.error(err);
                }
                await pool.close();

            } else {
                throw new Error(tableName)
            }
        }
        catch (err) {
            console.log(err);
        }

        // ======================================== //
        // ======================================== //
        // =========== Upload Sql File ============ //
        // ======================================== //
        // ======================================== //
        // Create file for office to store the JSON response and prep for upload
        // const blobExists = await blockBlobClient.exists();

        // if (blobExists) { return };
        console.log('uploading query file');
        await blockBlobClient.upload(query.toString(), query.toString().length);

        console.log('Connecting to sql')
        try {
            await pool.connect();
        } catch (err) {
            console.log('error opening connection to sql caught here')
            return;
        }

        console.log('executing query')
        pool.query(query).catch((err) => {
            console.log('ERROR IN QUERY CAUGHT HERE')
        });

        setTimeout(async () => {
            console.log('sql connection closed')
            try {
                await pool.close();
            } catch (err) {
                console.log('error closing connection to sql caught here')
            }
        }, 1000)
    }
}

module.exports = {
    authorizedPractices,
    clearBlobs,
    dataCheck,
    healthCheck,
    requestKey,
    requestKeyInfo,
    getBaseResourceByRequestKeyAndDumpToBlob,
    getSingleResourceByRequestKey
}

