const fetch = require('node-fetch');
const sql = require('mssql');
const queryString = require('query-string');
const uuidv4 = require('uuid/v4');

const { sikkaApi } = require('../SikkaApi');
const config = require('../config');

const {
  azureSqlConfig,
} = config;

module.exports = async function sqlDataDump(context) {
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
    const authorizedEndpoints = await sikkaApi.dataCheck(request_key.request_key);

    // loop through each authorized endpoint for this practice and drop the data in sql
    for (let endpointIndex = 0; endpointIndex < authorizedEndpoints[0].total_count; endpointIndex += 1) {
      const { api, total_records, update_time } = authorizedEndpoints[0].items[endpointIndex];

      context.log(`Api is: ${api}`);

      if (!api) {
        context.log(`There was no api defined ${api}`);
        continue;
      }

      const tableName = api.replace('/', '__');

      if (total_records === '0') {
        context.log(`There were no records for the ${api} api, total records: ${total_records}, last updated: ${update_time}`);
        continue;
      }

      // TODO, loop through while they have a next value. while => resourceResponse[0].pagination.next
      const resourceResponse = await sikkaApi.getBaseResourceByRequestKey(request_key.request_key, api);

      let table;

      for (let resourceIndex = 0; resourceIndex < resourceResponse[0].total_count; resourceIndex += 1) {
        const data = resourceResponse[0].items[resourceIndex];

        if (data) {
          try {
            const columns = [];
            Object.keys(data).forEach((key) => {
            //   context.log(`key: ${key}`);
              columns.push(`col_${key.toString().replace(/'/g, "''")}`);
            });

            const values = [];
            Object.keys(data).forEach((key) => {
              //   context.log(`data: ${data[key]}`);
              values.push(data[key]
            && data[key] !== ({} || []) ? data[key].toString().replace(/'/g, "''") : '');
            });

            table = new sql.Table(tableName);
            table.create = true;
            table.columns.add('UID', sql.UniqueIdentifier, { nullable: false, primary: true });
            table.columns.add('DATA', sql.VarChar(sql.MAX));
            // eslint-disable-next-line no-loop-func
            // columns.forEach((column) => {
            //   table.columns.add(column.toString(), sql.VarChar(sql.MAX), { nullable: true });
            // });
            table.rows.add(uuidv4(), JSON.stringify(data).replace(/'/g, "''"));
          } catch (err) {
            context.log(err);
          }
        } else {
          context.log('NO DATA');
          context.log(resourceIndex, resourceResponse[0]);
        }
      }

      try {
        context.log('Writing to SQL Database');
        const request = new sql.Request(pool);
        await request.bulk(table);
      } catch (err) {
        context.log(err); await pool.close();
        throw err;
      }
    }
  }

  await pool.close();

  context.log('Process Complete:');
  // eslint-disable-next-line no-param-reassign
  context.res = {
    status: 200,
    body: 'success',
  };
};
