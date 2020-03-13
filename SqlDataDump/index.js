const fetch = require("node-fetch");
const sql = require("mssql");
const queryString = require('query-string');

const { sikkaApi } = require('../SikkaApi');
const config = require('../config');

const {
    azureSqlConfig
} = config;

module.exports = async function (context, req) {

    context.log('JavaScript Authorized Practices function processed a request.');

    const pool = new sql.ConnectionPool(azureSqlConfig);
    await pool.connect();

    const authorizedPractices = await sikkaApi.authorizedPractices();

    context.log(`Authorized Practices: ${authorizedPractices[0].total_count}`);
    for (let i = 0; i < authorizedPractices[0].total_count; i += 1) {
        // loop through all practices

        const office = authorizedPractices[0].items[i];
        const { office_id, secret_key } = office;

        // get request key
        context.log(`Retrieving Request Key for Office ${office_id}`);
        const request_key = await sikkaApi.requestKey(office_id, secret_key);

        // get practice's authorized endpoints
        context.log(`Retrieving Authorized Endpoints For ${office_id}, Using The Request Key`);
        const data_check = await sikkaApi.dataCheck(request_key.request_key);

        // loop through each authorized endpoint for this practice and drop the data in sql
        for (let dchk = 0; dchk < data_check[0].total_count; dchk += 1) {
            const { api, total_records, update_time } = data_check[0].items[dchk]
            if (total_records == "0") {
                context.log(`There were no records for the ${api} api, total records: ${total_records}, last updated: ${update_time}`);
                continue;
            }

            const resourceResponse = await sikkaApi.getBaseResourceByRequestKey(request_key.request_key, api);
            const tableName = api.replace('/', '__');

            context.log('Writing to SQL Database');

            for (let resourceIndex = 0; resourceIndex < resourceResponse[0].total_count; resourceIndex += 1) {
                const data = resourceResponse[0].items[resourceIndex];

                context.log(data);

                if (data) {
                    let valuesString = '';
                    Object.keys(data).forEach(key => valuesString += `'${data[key] ? data[key].toString().replace(/'/g, "''") : ""}',`)

                    let columns = '';
                    Object.keys(data).forEach(key => columns += `[col_${key}], `)

                    // const query = `INSERT INTO ${tableName} (${columns} DATA) VALUES (${valuesString} '${JSON.stringify(data)}')`
                    const query = `INSERT INTO ${tableName} (${columns} DATA) VALUES (${valuesString} '${JSON.stringify(data).replace(/'/g, "''")}')`

                    try {
                        let columnsStringCreateTable = '';
                        Object.keys(data).forEach(key => columnsStringCreateTable += `col_${key} varchar(max),`)

                        if (tableName) {
                            await pool.query(
                                `
                            IF (object_id('${tableName}', 'U') is null) BEGIN
                            print 'Must create the table!';
                            CREATE TABLE ${tableName} (
                                    ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                                    ${columnsStringCreateTable}
                                    DATA varchar(max)
                                );
                            END
                        `
                            );
                        } else {
                            throw new Error(tableName)
                        }
                    }
                    catch (err) {
                        context.log(err);
                    }
                    try {
                        context.log(query);
                        await pool.query(query);
                    } catch (err) {
                        context.log(err); await pool.close()
                        return;
                    }
                } else {
                    context.log('NO DATA')
                    context.log(resourceIndex, resourceResponse[0]);
                }
            }
        }
    }

    await pool.close();

    context.log('Process Complete:');
    context.res = {
        status: 200,
        body: 'success'
    };
}