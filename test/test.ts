/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/node/node.d.ts" />

import fs = require('fs');
import path = require('path');
import assert = require('assert');
const bufferEqual = require('buffer-equal');

import AzureBlobStorage from '../index';


describe('Uploading various types of data to Azure', function() {
    this.timeout(10000);

    it('should initialize AzureBlobStorage object properly', (done) => {
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        done();
    });

    it('should upload JSON object to the storage, read it back and compare', (done) => {
        let objectToSend = { str: 'value', num: 85.543, bool: true, arr: [1, 2, 3], obj: { foo: 'bar' } };

        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.save('test-folder-1:object.json.gz', objectToSend).then(() => {
            storage.readAsObject('test-folder-1:object.json.gz').then((rcvdObject) => {
                assert.deepEqual(objectToSend, rcvdObject, 'Sent and received objects are not deep-equal');
                done();
            }).catch(done);
        }, done);
    });

    it('should upload Buffer to the storage, read it back and compare', (done) => {
        let buffer = fs.readFileSync(path.resolve(__dirname, 'pic.jpg'));

        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.save('test-folder-1:buffer.jpg', buffer).then(() => {
            storage.readAsBuffer('test-folder-1:buffer.jpg').then((rcvdBuffer) => {
                assert.ok(bufferEqual(buffer, rcvdBuffer), 'Sent and received buffers are not equal');
                done();
            }).catch(done);
        }, done);
    });

    it('should upload file from local filesystem to the storage, read it back and compare', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'),
            buffer = fs.readFileSync(fileName);

        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.save('test-folder-1:pic.jpg', fileName).then(() => {
            storage.readAsBuffer('test-folder-1:pic.jpg').then((rcvdBuffer) => {
                assert.ok(bufferEqual(buffer, rcvdBuffer), 'Sent and received buffers are not equal');
                done();
            }).catch(done);
        }, done);
    });

    it('should upload file from local filesystem to the storage, read it back and compare (with compression)', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'),
            buffer = fs.readFileSync(fileName);

        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.save('test-folder-1:pic.jpg', fileName, { compress: true }).then(() => {
            storage.readAsBuffer('test-folder-1:pic.jpg').then((rcvdBuffer) => {
                assert.ok(bufferEqual(buffer, rcvdBuffer), 'Sent and received buffers are not equal');
                done();
            }).catch(done);
        }, done);
    });

});

describe('Listing objects', function() {
    this.timeout(10000);

    it('should return an array of IBlobObjects', (done) => {
        let storage = new AzureBlobStorage(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.list('test-folder-1').then((list) => {
            assert.ok(Array.isArray(list), 'List should be an array');
            done();
        }).catch(done);
    });

});
