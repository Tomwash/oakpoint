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
        requestTimeout: 1000000,
        pool: {
            max: 1000,
            idleTimeoutMillis: 1000000
        }
    },
    azureStorageConfig: {
        connectionString: process.env['AzureWebJobsStorage'],
        containerName: 'oakpoint-data'
    },
    sikkaApiConfig: {
        baseUrl: 'https://api.sikkasoft.com/',
        authUrl: 'https://api.sikkasoft.com/auth/',
        version: 'v2/',
        app_id: process.env['APP_ID'],
        app_key: process.env['APP_KEY'],
    },
    tableCompositeKeys: {
        transactions: ['transaction_sr_no', 'procedure_code', 'patient_id', 'practice_id', 'provider_id', 'note']
    }
}