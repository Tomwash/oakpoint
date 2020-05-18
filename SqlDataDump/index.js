const fs = require('fs');
const { BlobServiceClient } = require('@azure/storage-blob');

const { sikkaApi } = require('../SikkaApi');
const config = require('../config');

const {
    azureStorageConfig: {
        connectionString,
        containerName
    },
    tables
} = config;

module.exports = async function (context) {
    context.log('JavaScript Authorized Practices function processed a request.');

    let authorizedPractices;
    try {
        authorizedPractices = await sikkaApi.authorizedPractices();
    } catch (err) {
        context.log(err);
    }

    if (!authorizedPractices) {
        context.log('authorizedPractices returned no response');
        context.log(authorizedPractices);
        return;
    }

    context.log(`Authorized Practices: ${authorizedPractices[0].total_count}`);
    const authorizedPracticesLimit = (authorizedPractices[0].total_count / 5000) < 1 ? (authorizedPractices[0].total_count % 5000) : 5000;

    context.log(`STARTING JOB ${new Date().toISOString()}`)
    for (let i = 0; i < authorizedPracticesLimit; i += 1) {
        context.log(`LOOPING THROUGH PRACTICES, Index ${i} of ${authorizedPracticesLimit} ${new Date().toISOString()}`)

        const office = authorizedPractices[0].items[i];
        const { office_id, secret_key, practice_name } = office;
        // if ([
        //     // 'D18336',
        //     // 'D22072',
        //     // 'D24510',
        //     // 'D34723',
        //     // // 'O22046',
        //     // // 'V15543'
        // ].includes(office_id)) {
        //     context.log(`Skipping ${office_id}`);
        //     continue;
        // }
        // get request key
        context.log(`Retrieving Request Key for Office ${office_id}`);
        const request_key = await sikkaApi.requestKey(office_id, secret_key);

        if (!request_key) {
            context.log('Request Key returned no response');
            context.log(request_key);
            continue;
        }

        // get practice's authorized endpoints
        context.log(`Retrieving Authorized Endpoints For ${office_id}, Using The Request Key`);

        // get practice's request key info
        context.log(`Retrieving Request Key Info ${office_id}, Using The Request Key`);
        const requestKeyInfo = await sikkaApi.requestKeyInfo(request_key.request_key);
        const listOfAccessibleApis = requestKeyInfo.scope.split(',')

        // loop through each authorized endpoint for this practice and drop the data in sql
        for (let j = 0; j < listOfAccessibleApis.length; j += 1) {
            if (['start', 'end'].includes(listOfAccessibleApis[j])) {
                continue;
            }
            context.log(`LOOPING THROUGH DATA CHECK, Index ${j} of ${listOfAccessibleApis.length} ${new Date().toISOString()}`)

            const tableName = listOfAccessibleApis[j].replace('/', '__');
            context.log(tableName);

            // // This is used to more easily step through tables we care about
            // if (!tables.includes(tableName)) {
            //     context.log(`skipping table ${tableName}`);
            //     continue;
            // }

            let resourceResponse;
            try {
                context.log(`FETCHING RESOURCE ${new Date().toISOString()}`)
                resourceResponse = await sikkaApi.getBaseResourceByRequestKeyAndDumpToBlob(request_key.request_key, listOfAccessibleApis[j], `streams/${office_id}/${tableName}.json`);
            } catch (err) {
                context.log(err);
            }
        }
    }

    context.log('Process Complete:');
    context.res = {
        status: 200,
        body: '\n\nsuccess\n\n'
    };
}