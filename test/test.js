/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/should/should.d.ts" />
/// <reference path="../typings/node/node.d.ts" />
"use strict";
var index_1 = require('../index');
describe('Uploading various types of data to Azure', () => {
    it('should initialize AzureBlobStorage properly', (done) => {
        let storage = new index_1.default('ConnectionString', 'TestContainerName');
    });
});
