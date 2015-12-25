/// <reference path='typings/node/node.d.ts' />
"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, Promise, generator) {
    return new Promise(function (resolve, reject) {
        generator = generator.call(thisArg, _arguments);
        function cast(value) { return value instanceof Promise && value.constructor === Promise ? value : new Promise(function (resolve) { resolve(value); }); }
        function onfulfill(value) { try { step("next", value); } catch (e) { reject(e); } }
        function onreject(value) { try { step("throw", value); } catch (e) { reject(e); } }
        function step(verb, value) {
            var result = generator[verb](value);
            result.done ? resolve(result.value) : cast(result.value).then(onfulfill, onreject);
        }
        step("next", void 0);
    });
};
var stream = require('stream');
var fs = require('fs');
const azure = require('azure-storage');
const promisify = require('es6-promisify');
const FOLDER_SEPARATOR = ':';
class AzureBlobStorage {
    /**
     * Optional folderName parameter can be set in order to use shortcut save/read methods
     */
    constructor(connectionString, containerName, verbose) {
        this.log = verbose ? console.log.bind(console) : () => void 0;
        this.blobService = azure.createBlobService(connectionString);
        this.blobStorageContainerName = containerName;
    }
    save(folderName, name, object, options) {
        return __awaiter(this, void 0, Promise, function* () {
            let fullBlobName = [folderName, name].join(FOLDER_SEPARATOR), blobOptions = {
                metadata: {}
            };
            let readableStream, readableStreamLength;
            if (object instanceof stream.Readable) {
                this.log('Object type: stream');
                if (!options || !options['length']) {
                    throw new Error('Stream length is required');
                }
                readableStream = object;
                readableStreamLength = +options['length'];
                blobOptions.metadata['type'] = 'binary';
            }
            else if (object instanceof Buffer) {
                this.log('Object type: buffer');
                readableStream = new stream.Readable();
                readableStream._read = () => { };
                readableStream.push(object);
                readableStream.push(null);
                readableStreamLength = object.length;
                blobOptions.metadata['type'] = 'binary';
            }
            else if (typeof object === 'string') {
                this.log('Object type: local filename');
                // Will throw error in case file is not exists
                let fileStat = fs.statSync(object);
                readableStream = fs.createReadStream(object);
                readableStreamLength = fileStat.size;
                blobOptions.metadata['type'] = 'binary';
            }
            else if (object instanceof Object) {
                this.log('Object type: json');
                let stringData = JSON.stringify(object, null, 0);
                readableStream = new stream.Readable();
                readableStream._read = () => { };
                readableStream.push(stringData);
                readableStream.push(null);
                readableStreamLength = stringData.length;
                blobOptions.metadata['type'] = 'json';
            }
            else {
                throw new Error('Unsupported object type');
            }
            if (options && options['contentType']) {
                this.log(`Setting contentType for the blob: ${options['contentType']}`);
                blobOptions['contentType'] = options['contentType'];
            }
            this.log(`Stream length: ${readableStreamLength}`);
            yield promisify(this.blobService.createContainerIfNotExists.bind(this.blobService))(this.blobStorageContainerName, { publicAccessLevel: 'blob' });
            yield promisify(this.blobService.createBlockBlobFromStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, readableStream, readableStreamLength, blobOptions);
        });
    }
    read(folderName, name, writeStream) {
        return __awaiter(this, void 0, Promise, function* () {
            let fullBlobName = [folderName, name].join(FOLDER_SEPARATOR);
            yield promisify(this.blobService.getBlobToStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, writeStream);
        });
    }
    readAsBuffer(folderName, name) {
        return __awaiter(this, void 0, Promise, function* () {
            throw new Error('not yet implemented');
        });
    }
    readAsObject(folderName, name) {
        return __awaiter(this, void 0, Promise, function* () {
            let fullBlobName = [folderName, name].join(FOLDER_SEPARATOR);
            let result = yield promisify(this.blobService.getBlobToText.bind(this.blobService))(this.blobStorageContainerName, fullBlobName), text = result[0], metadata = result[1].metadata;
            if (metadata.type !== 'json') {
                throw new Error('The requested blob can\'t be downloaded as JSON object');
            }
            return JSON.parse(text);
        });
    }
}
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = AzureBlobStorage;
