const df = require("durable-functions");

const { sikkaApi } = require('../SikkaApi');

module.exports = async function (context, req) {

    context.log('JavaScript Authorized Practices function processed a request.');

    context.res = {
        body: context.bindings.result
    }
    return;

    const authorizedPractices = await sikkaApi.authorizedPractices();

    const offices = [];

    context.log(`Authorized Practices: ${authorizedPractices[0].total_count}`);
    for (let i = 0; i < authorizedPractices[0].total_count; i += 1) {
        const office = authorizedPractices[0].items[i];
        const { office_id, secret_key } = office;

        context.log(`Retrieving Request Key for Office ${office_id}`);
        const request_key = await sikkaApi.requestKey(office_id, secret_key);

        context.log(`Retrieving Authorized Endpoints For ${office_id}, Using The Request Key`);
        const data_check = await sikkaApi.dataCheck(request_key.request_key);

        offices.push({ office, request_key, data_check })
    }

    context.res = {
        body: {
            offices
        }
    };
}