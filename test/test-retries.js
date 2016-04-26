/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/node/node.d.ts" />
"use strict";
const fs = require('fs');
const path = require('path');
const nock = require('nock');
const AzureBlobStorage = require('../index');
const TEST_TIMEOUT = 30000;
const logger = console.log.bind(console);
describe('Upload object with retries', function () {
    this.timeout(TEST_TIMEOUT);
    // Disable network
    nock.disableNetConnect();
    // Enable network after 10000 ms
    setTimeout(nock.enableNetConnect, 10000);
    it('should upload image with specified content type with 5 retries', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'), buffer = fs.readFileSync(fileName), contentType = 'image/jpeg';
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.setRetriesCount(5, 5000);
        storage.save('test-folder-1:pic.jpg', fileName, { contentType: contentType, getURL: true }).then((url) => {
            console.log('Got URL:', url);
            done();
        }).catch(done);
    });
});
