const fetch = require("node-fetch");
const sql = require("mssql");

const pool = new sql.ConnectionPool({
    server: process.env['AZURE_SQL_SERVER'], // Use your SQL server name
    database: process.env['AZURE_SQL_SERVER_DATABASE_PRACTICE_DATA'], // Database to connect to
    user: process.env['AZURE_SQL_SERVER_DATABASE_PRACTICE_DATA_USERNAME'], // Use your username
    password: process.env['AZURE_SQL_SERVER_DATABASE_PRACTICE_DATA_PASSWORD'], // Use your password
    port: 1433,
    // Since we're on Windows Azure, we need to set the following options
    options: {
        encrypt: true
    }
});

module.exports = async function (context, req) {
    context.log('JavaScript Authorized Practices function processed a request.');

    const appId = process.env['APP_ID'];
    const appKey = process.env['APP_KEY'];

    const url = `https://api.sikkasoft.com/auth/v2/authorized_practices?app_id=${appId}&app_key=${appKey}`;

    const response = await fetch(url);
    const json = await response.json();
    context.log(json);

    context.log('Writing to SQL Database');

    await pool.connect();

    const practicesTable = 'dbo.practices';

    context.log(json[0].items.length);
    for (let i = 0; i < json[0].items.length; i += 1) {
        const practice = json[0].items[i];
        context.log(practice);
        const query = `INSERT INTO ${practicesTable} (PracticeID, JSON) VALUES (${practice.practice_id}, '${JSON.stringify(practice)}')`;

        context.log(query);
        try { await pool.query(query); } catch (err) { context.log(err); await pool.close() }
    }

    await pool.close();

    context.res = {
        body: json
    };
}