/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/should/should.d.ts" />
/// <reference path="../typings/node/node.d.ts" />

import fs = require('fs');
require('should');

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
        storage.save('test-folder-1', 'object.json', objectToSend).then(() => {
            storage.readAsObject('test-folder-1', 'object.json').then((rcvdObject) => {
                objectToSend.should.eql(rcvdObject);
                done();
            }, done);
        }, done);
    });

});
