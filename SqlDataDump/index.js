const fetch = require("node-fetch");
const sql = require("mssql");
const queryString = require('query-string');

const { sikkaApi } = require('../SikkaApi');
const config = require('../config');

const {
    azureSqlConfig
} = config;

const currentDate = new Date();
const currentIsoDateTime = currentDate.toISOString();

module.exports = async function (context, req) {

    // context.log(`CURRENT DATE ${currentIsoDateTime}\n\n`);

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
    // const queries = [];
    context.log(`Authorized Practices: ${authorizedPractices[0].total_count}`);
    const authorizedPracticesLimit = authorizedPractices[0].total_count < 5000 ? authorizedPractices[0].total_count : 5000;

    context.log(`STARTING JOB ${new Date().toISOString()}`)
    for (let i = 0; i < authorizedPracticesLimit; i += 1) {
        // loop through all practices

        context.log(`LOOPING THROUGH PRACTICES, Index ${i} of ${authorizedPracticesLimit} ${new Date().toISOString()}`)

        const office = authorizedPractices[0].items[i];
        const { office_id, secret_key, practice_name } = office;

        // if (office_id !== 'D22072') {
        //     context.log(`skipping office id: ${office_id}`)
        //     continue;
        // }

        if (office_id !== 'D18336') {
            context.log(`skipping office id: ${office_id}`)
            continue;
        }

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

        let data_check;
        try {
            data_check = await sikkaApi.dataCheck(request_key.request_key);
        } catch (err) {
            context.log(err);
        }

        if (!data_check) {
            context.log('Data check returned no response');
            context.log(data_check);
            continue;
        }

        const dataCheckLimit = data_check[0].total_count < 5000 ? data_check[0].total_count : 5000;
        // loop through each authorized endpoint for this practice and drop the data in sql
        for (let dchk = 0; dchk < dataCheckLimit; dchk += 1) {
            context.log(`LOOPING THROUGH DATA CHECK, Index ${dchk} of ${dataCheckLimit} ${new Date().toISOString()}`)

            const { api, total_records, update_time } = data_check[0].items[dchk]
            if (total_records == "0") {
                context.log(`There were no records for the ${api} api, total records: ${total_records}, last updated: ${update_time}`);
                continue;
            }

            let resourceResponse;
            try {
                context.log(`FETCHING RESOURCE ${new Date().toISOString()}`)
                resourceResponse = await sikkaApi.getBaseResourceByRequestKey(request_key.request_key, api);
            } catch (err) {
                context.log(err);
            }

            if (!resourceResponse || (resourceResponse && !resourceResponse[0])) {
                context.log(resourceResponse);
                context.log('resourceResponse api returned no response');
                continue;
            }

            context.log(`Opening Connection to SQL Database: ${new Date().toISOString()}`);
            const pool = new sql.ConnectionPool(azureSqlConfig);
            await pool.connect();
            context.log(`Connection to SQL Database is open: ${new Date().toISOString()}`);

            const tableName = api.replace('/', '__');
            context.log(tableName);

            const resourceResponseLimit = resourceResponse[0].total_count < 5000 ? resourceResponse[0].total_count : 5000;
            for (let resourceIndex = 0; resourceIndex < resourceResponseLimit; resourceIndex += 1) {
                context.log(`EXECUTING QUERY FOR Resource Record ${resourceIndex} of ${resourceResponseLimit}  ${new Date().toISOString()}`)
                const data = resourceResponse[0].items[resourceIndex];

                if (data) {
                    let valuesString = '';
                    let columns = '';
                    let setString = '';
                    let columnsStringCreateTable = '';
                    Object.keys(data).forEach(key => {
                        valuesString += `'${data[key] ? data[key].toString().replace(/'/g, "''") : ""}',`;
                        columns += `[col_${key}], `;
                        setString += `[col_${key}] = '${data[key] ? data[key].toString().replace(/'/g, "''") : ""}', `;
                        columnsStringCreateTable += `col_${key} varchar(2000),`;
                    });

                    setString += `
                            updated_at = '${currentIsoDateTime}',
                            office_id = '${office_id}',
                            practice_name = '${practice_name.replace(/'/g, "''")}'
                        `

                    // const query = `INSERT INTO ${tableName} (${columns} DATA) VALUES (${valuesString} '${JSON.stringify(data).replace(/'/g, "''")}')`;
                    const query = `
                        BEGIN TRY
                         
                            INSERT INTO ${tableName} 
                            (
                                ${columns} 
                                DATA, 
                                created_at,
                                updated_at,
                                office_id,
                                practice_name
                            ) 
                            VALUES 
                            (
                                ${valuesString} 
                                '${JSON.stringify(data).replace(/'/g, "''")}', 
                                '${currentIsoDateTime}', 
                                '${currentIsoDateTime}', 
                                '${office_id}', 
                                '${practice_name.replace(/'/g, "''")}'
                            );
                         
                        END TRY
                         
                        BEGIN CATCH
                         
                          -- ignore duplicate key errors, throw the rest.
                          IF ERROR_NUMBER() IN (2601, 2627) 
                            UPDATE dbo.${tableName}
                               SET ${setString}, DATA = '${JSON.stringify(data).replace(/'/g, "''")}'
                             WHERE col_href = '${data['href']}';
                         
                        END CATCH
                    `
                    // context.log(`============\n\n${query}\n\n================`)
                    try {
                        if (resourceIndex === 0) {
                            if (tableName) {
                                const createTableQuery =
                                    `
                            IF (object_id('${tableName}', 'U') is null) BEGIN
                                print 'Must create the table!';
                                CREATE TABLE ${tableName} (
                                    ID bigint NOT NULL PRIMARY KEY IDENTITY(1, 1),
                                    ${columnsStringCreateTable}
                                    DATA varchar(max),
                                    created_at datetime,
                                    updated_at datetime,
                                    office_id varchar(2000),
                                    practice_name varchar(2000)
                                );
                                
                                CREATE UNIQUE INDEX col_href
                                ON ${tableName} (col_href)
                            END
                        `

                                // context.log(`&&&&&&&&&&&&&&&&&&\n\n${createTableQuery}\n\n&&&&&&&&&&&&&&&&&&`)
                                context.log(`EXECUTING CREATE TABLE QUERY ${new Date().toISOString()}`)
                                await pool.query(createTableQuery);
                                context.log(`EXECUTING CREATE TABLE QUERY COMPLETE ${new Date().toISOString()}`)
                            } else {
                                throw new Error(tableName)
                            }
                        }
                    }
                    catch (err) {
                        context.log(err);
                        context.log(`Closing connection to database: ${new Date().toISOString()}`)
                        await pool.close();
                    }

                    try {
                        await pool.query(query);
                    } catch (err) {
                        context.log(err);
                        context.log(`Closing connection to database: ${new Date().toISOString()}`)
                        await pool.close();
                        return;
                    }
                } else {
                    context.log('NO DATA')
                    context.log(resourceResponse)
                    context.log(`Index: ${resourceIndex}`)
                }
            } // end resource loops
            context.log(`Closing connection to database: ${new Date().toISOString()}`)
            await pool.close();
        }
    }
    // context.log(`Number of queries ${queries.length}`)

    context.log('Process Complete:');
    context.res = {
        status: 200,
        body: 'success'
    };
}