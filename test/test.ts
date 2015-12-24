/// <reference path="../typings/mocha/mocha.d.ts" />
/// <reference path="../typings/should/should.d.ts" />
/// <reference path="../typings/node/node.d.ts" />

import AzureBlobStorage from '../index';

describe('Uploading various types of data to Azure', () => {
	it('should initialize AzureBlobStorage properly', (done) => {
		let storage = new AzureBlobStorage('ConnectionString', 'TestContainerName');
	});
});
