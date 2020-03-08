const fetch = require("node-fetch");
const sql = require("mssql");
const queryString = require('query-string');

const config = require('../config');

const {
    azureSqlConfig,
    sikkaApiConfig: {
        appId,
        appKey
    }
} = config;

module.exports = async function (context, req) {
    const {
        body: {
            apiQueryParams,
            query: {
                tableName,
                columns,
                values,
                type,
            }
        }
    } = req;

    apiQueryParams.app_id = appId;
    apiQueryParams.app_key = appKey;

    const pool = new sql.ConnectionPool(azureSqlConfig);
    await pool.connect();

    context.log('JavaScript Authorized Practices function processed a request.');

    const url = `https://api.sikkasoft.com/auth/v2/authorized_practices?${queryString.stringify(apiQueryParams)}`;

    const response = await fetch(url);
    const json = await response.json();

    context.log('Writing to SQL Database');

    for (let i = 0; i < json[0].items.length; i += 1) {
        const data = json[0].items[i];

        let query;
        switch (type) {
            case 'INSERT':
                query = `INSERT INTO ${tableName} (${columns.join()}) VALUES (${values.join()}, ${JSON.stringify(data)})`
                break;
        }

        try {
            await pool.query(query);
        } catch (err) {
            context.log(err); await pool.close()
        }
    }

    await pool.close();

    context.res = {
        body: json
    };
}