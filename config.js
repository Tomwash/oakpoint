module.exports = {
    azureSqlConfig: {
        server: process.env['AZURE_SQL_SERVER'], // Use your SQL server name
        database: process.env['AZURE_SQL_SERVER_DATABASE_PRACTICE_DATA'], // Database to connect to
        user: process.env['AZURE_SQL_SERVER_DATABASE_PRACTICE_DATA_USERNAME'], // Use your username
        password: process.env['AZURE_SQL_SERVER_DATABASE_PRACTICE_DATA_PASSWORD'], // Use your password
        port: 1433,
        // Since we're on Windows Azure, we need to set the following options
        options: {
            encrypt: true
        },
    },
    sikkaApiConfig: {
        appId: process.env['APP_ID'],
        appKey: process.env['APP_KEY'],
    }
}