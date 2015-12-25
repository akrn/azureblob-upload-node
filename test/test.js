/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/should/should.d.ts" />
/// <reference path="../typings/node/node.d.ts" />
"use strict";
var fs = require('fs');
var path = require('path');
const bufferEqual = require('buffer-equal');
require('should');
var index_1 = require('../index');
describe('Uploading various types of data to Azure', function () {
    this.timeout(10000);
    it('should initialize AzureBlobStorage object properly', (done) => {
        let storage = new index_1.default(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        done();
    });
    it('should upload JSON object to the storage, read it back and compare', (done) => {
        let objectToSend = { str: 'value', num: 85.543, bool: true, arr: [1, 2, 3], obj: { foo: 'bar' } };
        let storage = new index_1.default(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.save('test-folder-1', 'object.json', objectToSend).then(() => {
            storage.readAsObject('test-folder-1', 'object.json').then((rcvdObject) => {
                objectToSend.should.eql(rcvdObject);
                done();
            }, done);
        }, done);
    });
    it('should upload Buffer to the storage, read it back and compare', (done) => {
        let buffer = fs.readFileSync(path.resolve(__dirname, 'pic.jpg'));
        let storage = new index_1.default(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.save('test-folder-1', 'buffer.jpg', buffer).then(() => {
            storage.readAsBuffer('test-folder-1', 'buffer.jpg').then((rcvdBuffer) => {
                bufferEqual(buffer, rcvdBuffer).should.ok();
                done();
            }, done);
        }, done);
    });
    it('should upload file from local filesystem to the storage, read it back and compare', (done) => {
        let fileName = path.resolve(__dirname, 'pic.jpg'), buffer = fs.readFileSync(fileName);
        let storage = new index_1.default(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.save('test-folder-1', 'pic.jpg', fileName).then(() => {
            storage.readAsBuffer('test-folder-1', 'pic.jpg').then((rcvdBuffer) => {
                bufferEqual(buffer, rcvdBuffer).should.ok();
                done();
            }, done);
        }, done);
    });
});
