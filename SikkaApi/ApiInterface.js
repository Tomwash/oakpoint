const fetch = require("node-fetch");
const queryString = require('query-string');

const config = require('../config');

const {
    sikkaApiConfig: {
        authUrl,
        baseUrl,
        version,
        app_id,
        app_key
    }
} = config;

async function get(url) {
    const response = await fetch(url);
    const json = await response.json();

    return json;
}

//SPC Support API
async function healthCheck(officeId) {
    const url = `${authUrl}${version}health_check/${officeId}`;
    return get(url);
}

// Authentication API
async function applications() {
    const url = `${authUrl}${version}applications?${queryString.stringify({ app_id, app_key })}`;
    return get(url);
}

async function authorizedPractices() {
    const url = `${authUrl}${version}authorized_practices?${queryString.stringify({ app_id, app_key })}`;
    return get(url);
}

async function authorizedPracticesByOfficeId(office_id) {
    const url = `${authUrl}${version}authorized_practices/${office_id}?${queryString.stringify({ app_id, app_key })}`;
    return get(url);
}

async function requestKeyInfo(request_key) {
    const url = `${baseUrl}${version}request_key_info?${queryString.stringify({ request_key })}`;
    return get(url);
}

async function requestKey(office_id, secret_key) {
    const url = `${authUrl}${version}request_keys?${queryString.stringify({ app_id, app_key, office_id, secret_key })}`;
    return get(url);
}

async function requestKeyDetails(request_key) {
    const url = `${authUrl}${version}request_keys_details/${request_key}?${queryString.stringify({ app_id, app_key })}`;
    return get(url);
}

async function dataCheck(request_key) {
    const url = `${baseUrl}${version}data_check?${queryString.stringify({ request_key })}`;
    return get(url);
}

async function getBaseResourceByRequestKey(request_key, resourceUri) {
    const url = `${baseUrl}${version}${resourceUri}?${queryString.stringify({ request_key, limit: 5000, offset: 1 })}`;
    return get(url);
}

module.exports = {
    authorizedPractices,
    dataCheck,
    healthCheck,
    requestKey,
    getBaseResourceByRequestKey,
}

