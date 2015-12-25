/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/should/should.d.ts" />
/// <reference path="../typings/node/node.d.ts" />
"use strict";
var index_1 = require('../index');
require('should');
describe('Uploading various types of data to Azure', function () {
    this.timeout(10000);
    it('should initialize AzureBlobStorage object properly', (done) => {
        let storage = new index_1.default(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        done();
    });
    it('should upload JSON object to the storage', (done) => {
        let storage = new index_1.default(process.env.AZURE_STORAGE_CONNECTION_STRING, 'test-container', true);
        storage.save('test-folder-1', 'object.json', { key: 'value' })
            .then(done, done)
            .catch((e) => {
            e.message.should.be.equal('fail');
        });
    });
});
