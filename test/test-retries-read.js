/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/node/node.d.ts" />
"use strict";
var fs = require('fs');
var path = require('path');
var assert = require('assert');
const nock = require('nock');
const bufferEqual = require('buffer-equal');
var AzureBlobStorage = require('../index');
const TEST_TIMEOUT = 30000;
describe('Read object with retries', function () {
    this.timeout(TEST_TIMEOUT);
    it('should read image with 5 retries', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'), blobName = 'test-folder-1:pic.jpg', buffer = fs.readFileSync(fileName), contentType = 'image/jpeg';
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.setRetriesCount(5, 5000);
        storage.save(blobName, fileName, { contentType: contentType }).then(() => {
            // Disable network
            nock.disableNetConnect();
            // Enable network after 10000 ms
            setTimeout(nock.enableNetConnect, 10000);
            storage.readAsBuffer(blobName).then((rcvdBuffer) => {
                assert.ok(bufferEqual(buffer, rcvdBuffer), 'Sent and received buffers are not equal');
                done();
            }).catch(done);
        }).catch(done);
    });
});
