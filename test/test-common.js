/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/node/node.d.ts" />
/// <reference path="../typings/request/request.d.ts" />
"use strict";
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const request = require('request');
const bufferEqual = require('buffer-equal');
const AzureBlobStorage = require('../index');
const TEST_TIMEOUT = 30000;
const logger = console.log.bind(console);
describe('Uploading various types of data to Azure', function () {
    this.timeout(TEST_TIMEOUT);
    it('should initialize AzureBlobStorage object properly', (done) => {
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container');
        done();
    });
    it('should upload JSON object to the storage, read it back and compare', (done) => {
        let objectToSend = { str: 'value', num: 85.543, bool: true, arr: [1, 2, 3], obj: { foo: 'bar' } };
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.save('test-folder-1:object.json.gz', objectToSend).then(() => {
            storage.readAsObject('test-folder-1:object.json.gz').then((rcvdObject) => {
                assert.deepEqual(objectToSend, rcvdObject, 'Sent and received objects are not deep-equal');
                done();
            }).catch(done);
        }).catch(done);
    });
    it('should upload Buffer to the storage, read it back and compare', (done) => {
        let buffer = fs.readFileSync(path.resolve(__dirname, 'pic.jpg'));
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.save('test-folder-1:buffer.jpg', buffer).then(() => {
            storage.readAsBuffer('test-folder-1:buffer.jpg').then((rcvdBuffer) => {
                assert.ok(bufferEqual(buffer, rcvdBuffer), 'Sent and received buffers are not equal');
                done();
            }).catch(done);
        }).catch(done);
    });
    it('should upload file from local filesystem to the storage, read it back and compare', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'), buffer = fs.readFileSync(fileName);
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.save('test-folder-1:pic.jpg', fileName).then(() => {
            storage.readAsBuffer('test-folder-1:pic.jpg').then((rcvdBuffer) => {
                assert.ok(bufferEqual(buffer, rcvdBuffer), 'Sent and received buffers are not equal');
                done();
            }).catch(done);
        }).catch(done);
    });
    it('should upload file from local filesystem to the storage using stream, read it back and compare', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'), stream = fs.createReadStream(fileName), buffer = fs.readFileSync(fileName);
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.save('test-folder-1:pic.jpg', stream, { streamLength: buffer.length }).then(() => {
            storage.readAsBuffer('test-folder-1:pic.jpg').then((rcvdBuffer) => {
                assert.ok(bufferEqual(buffer, rcvdBuffer), 'Sent and received buffers are not equal');
                done();
            }).catch(done);
        }).catch(done);
    });
    it('should upload file from local filesystem to the storage, read it back and compare (with compression)', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'), buffer = fs.readFileSync(fileName);
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.save('test-folder-1:pic.jpg', fileName, { compress: true }).then(() => {
            storage.readAsBuffer('test-folder-1:pic.jpg').then((rcvdBuffer) => {
                assert.ok(bufferEqual(buffer, rcvdBuffer), 'Sent and received buffers are not equal');
                done();
            }).catch(done);
        }).catch(done);
    });
});
describe('Listing objects', function () {
    this.timeout(TEST_TIMEOUT);
    it('should return an array of IBlobObjects with specified prefix', (done) => {
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.list('test-folder-1:').then((list) => {
            assert.ok(Array.isArray(list), 'List should be an array');
            list.forEach((item) => assert.ok(item.fullBlobName.startsWith('test-folder-1'), 'Results contain item(s) from other folders'));
            done();
        }).catch(done);
    });
});
describe('Upload object and retrieve URL', function () {
    this.timeout(TEST_TIMEOUT);
    it('should upload image with specified content type and then retrieve it via HTTP', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'), buffer = fs.readFileSync(fileName), contentType = 'image/jpeg';
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        storage.save('test-folder-1:pic.jpg', fileName, { contentType: contentType, getURL: true }).then((url) => {
            assert.ok(typeof url === 'string', 'Blob URL should be a string');
            console.log('Got URL:', url);
            request.get(url, { encoding: null }, (err, res, body) => {
                assert.equal(res.statusCode, 200, 'Status code was not 200');
                assert.equal(res.headers['content-type'], contentType, 'Content type is wrong for retrieved blob');
                assert.ok(bufferEqual(body, buffer), 'Sent object is not equal with object retrieved by URL');
                done();
            });
        }).catch(done);
    });
});
describe('Upload object with additional metadata', function () {
    this.timeout(TEST_TIMEOUT);
    it('should upload an object with additional metadata and then access the metadata via list()', (done) => {
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', logger);
        let fullBlobName = 'test-folder-1:metadata.json', metadataValue = Math.random().toString();
        storage.save(fullBlobName, { a: 1 }, { metadata: { key: metadataValue } }).then(() => {
            storage.list('test-folder-1:').then((list) => {
                let blob = list.find((item) => item.fullBlobName === fullBlobName);
                assert.ok(blob.metadata['key'] === metadataValue, 'Sent and received metadata value should be equal');
                done();
            }).catch(done);
        }).catch(done);
    });
});
