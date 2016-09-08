/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/node/node.d.ts" />

import fs = require('fs');
import path = require('path');
import assert = require('assert');
const nock = require('nock');
const bufferEqual = require('buffer-equal');

import AzureBlobStorage = require('../index');

const TEST_TIMEOUT = 30000;
const logger = console.log.bind(console);


describe('Read 3 objects + write 1 object in parallel with retries', function() {
    this.timeout(TEST_TIMEOUT);

    it('should read image with 5 retries', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'),
            blobName = 'test-folder-1:pic.jpg',
            buffer = fs.readFileSync(fileName),
            contentType = 'image/jpeg';

        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.setRetriesCount(5, 5000);

        storage.save(blobName, fileName, { contentType: contentType }).then(() => {
            // Disable network
            nock.disableNetConnect();

            // Enable network after 10000 ms
            setTimeout(nock.enableNetConnect, 10000);

            let promises = [
                storage.readAsBuffer(blobName),
                storage.readAsBuffer(blobName),
                storage.readAsBuffer(blobName),
                storage.save('test-folder-1:object.json', { a: 1 })
            ];

            Promise.all(promises).then(rcvdBuffers => {
                [0, 1, 2].forEach(idx => assert.ok(bufferEqual(buffer, rcvdBuffers[idx]), 'Sent and received buffers are not equal'));
                done();
            }).catch(done);
        }).catch(done);
    });

});