const fetch = require("node-fetch");
const queryString = require('query-string');
const getStream = require('into-stream');
const { v4: uuidV4 } = require('uuid');

const {
    BlobServiceClient
} = require('@azure/storage-blob');

const config = require('../config');

const {
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

async function getBaseResourceByRequestKeyAndDumpToBlob(request_key, resourceUri, blobLocation, limit = 5000, offset = 0) {

    let data;
    const url = `${baseUrl}${version}${resourceUri}?${queryString.stringify({ request_key, limit, offset })}`;
    try {
        data = await get(url);
    } catch (err) {
        console.log("WARNING: ERROR IN MAIN REQUEST, PROBABLY A 204", data, err)
        return [];
    }

    if (data[0]) {
        const items = data[0].items.map((item) => {
            return JSON.stringify(item)
        });
        const listOfBlockIds = [];

        let blockId;

        blockId = Buffer.from(uuidV4()).toString('base64');

        console.log('writing stream in staged blocks before loop', items.length);
        await writeStreamInStagedBlocks(blockId, items.join(','), blobLocation);
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
                    const itemsInLoop = data[0].items.map((item) => {
                        return JSON.stringify(item)
                    });
                    console.log('writing stream in staged blocks inside loop', itemsInLoop.length);

                    blockId = Buffer.from(uuidV4()).toString('base64');
                    const stageResult = await writeStreamInStagedBlocks(blockId, itemsInLoop.join(','), blobLocation);
                    listOfBlockIds.push(blockId);
                    // console.log(`stage of blockId: ${blockId} successful\n\n\n current blockid list values are: ${listOfBlockIds}\n\n\n stage block response \n\n`);
                    // console.log(stageResult);
                } catch (err) {
                    console.log(err);
                }
            }
        }

        const commitBlocksResult = await commitStagedBlocks(listOfBlockIds, blobLocation);
        console.log(`commit of blockid list successful:\n\n ${listOfBlockIds}\n\n\n commit block response \n\n`);
        // console.log(commitBlocksResult);

        return [];
    }

    return [];

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
    const blockBlobClient = await containerClient.getBlockBlobClient(blobLocation)

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



module.exports = {
    authorizedPractices,
    dataCheck,
    healthCheck,
    requestKey,
    requestKeyInfo,
    getBaseResourceByRequestKeyAndDumpToBlob,
    getSingleResourceByRequestKey
}

