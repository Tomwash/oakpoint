
const sql = require("mssql");
const {
    BlobServiceClient
} = require('@azure/storage-blob');

const { sikkaApi } = require('../SikkaApi');
const config = require('../config');
const {
    azureSqlConfig,
    azureStorageConfig: {
        connectionString,
        containerName
    }
} = config;

module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();

    if (myTimer.IsPastDue) {
        context.log('Node is running late!');
    }
    context.log('Node timer trigger function started!', timeStamp);

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

    context.log('Clearing Blobs');
    // await sikkaApi.clearBlobs();

    context.log(`Authorized Practices: ${authorizedPractices[0].total_count}`);
    const authorizedPracticesLimit = authorizedPractices[0].total_count;

    context.log(`STARTING JOB ${new Date().toISOString()}`)
    for (let i = 0; i < authorizedPractices[0].total_count; i += 1) {
        context.log(`LOOPING THROUGH PRACTICES, Index ${i} of ${authorizedPracticesLimit} ${new Date().toISOString()}`)

        const office = authorizedPractices[0].items[i];
        const { office_id, secret_key, practice_name } = office;

        context.log(`Retrieving Request Key for Office ${office_id}`);
        let request_key;
        try {
            request_key = await sikkaApi.requestKey(office_id, secret_key);
        } catch (err) {
            context.log(err);
            throw err;
        }

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

            let resourceResponse;
            try {
                context.log(`FETCHING RESOURCE ${new Date().toISOString()}`)
                resourceResponse = await sikkaApi.getBaseResourceByRequestKeyAndDumpToBlob(request_key.request_key, listOfAccessibleApis[j], `streams-recent/${office_id}/${tableName}.json`, { office_id, practice_name, tableName });
            } catch (err) {
                context.log(err);
            }
        }
    }

    context.log('Process Complete, firing off email');

    var message = {
        "personalizations": [{ "to": [{ "email": "tommyjohn2006@gmail.com" }, { "email": "asif.asim212@gmail.com" }, { "email": "asharma@oakpoint.us" }] }],
        from: { email: "asharma@oakpoint.us" },
        subject: "ETL Complete",
        content: [{
            type: 'text/plain',
            value: 'ETL ran successfully'
        }]
    };

    context.done(null, message);
}