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

    context.log('JavaScript Merge Data processed a request.');

    // Create the BlobServiceClient object which will be used to create a container client
    const blobServiceClient = await BlobServiceClient.fromConnectionString(connectionString);

    // // Get a reference to a container
    const containerClient = await blobServiceClient.getContainerClient(containerName);

    const queries = [];
    for await (const blob of containerClient.listBlobsFlat()) {
        if (blob.name.includes('streams')) {
            continue;
        }
        console.log(blob.name);

        const blockBlobClient = await containerClient.getBlockBlobClient(blob.name);
        const query = await blockBlobClient.download();

        console.log(query);
        // queries.push(new Promise( async (resolve, reject) => {
        //     const pool = new sql.ConnectionPool(azureSqlConfig);
        //     await pool.connect();

        //     const blockBlobClient = await containerClient.getBlockBlobClient(blob.name);
        //     const query = await blockBlobClient.download();

        //     console.log(query);

        //     await pool.close();
        // }));
    }

    return;


    let responseFromAllPromises;
    try {
        responseFromAllPromises = await Promise.all(queries);
    } catch (err) {
        console.log('error in promises', err)
    }

    context.log('Process Complete:');
    context.res = {
        status: 200,
        body: 'success'
    };
}