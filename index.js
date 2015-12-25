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
    save(folderName, name, object) {
        return __awaiter(this, void 0, Promise, function* () {
            let readableStream, readableStreamLength;
            if (object instanceof stream.Readable) {
                throw new Error('not yet implemented');
            }
            else if (object instanceof Buffer) {
                throw new Error('not yet implemented');
            }
            else if (typeof object === 'string') {
                throw new Error('not yet implemented');
            }
            else if (object instanceof Object) {
                let stringData = JSON.stringify(object, null, 0);
                readableStream = new stream.Readable();
                readableStream._read = () => { };
                readableStream.push(stringData);
                readableStreamLength = stringData.length;
            }
            else {
                throw new Error('Unsupported object type');
            }
            let fullName = [folderName, name].join(FOLDER_SEPARATOR);
            let result = yield promisify(this.blobService.createContainerIfNotExists.bind(this.blobService))(this.blobStorageContainerName, {
                publicAccessLevel: 'blob'
            });
            this.log(`createContainerIfNotExists(${this.blobStorageContainerName}) result:`, result);
            yield promisify(this.blobService.createBlockBlobFromStream.bind(this.blobService))(this.blobStorageContainerName, fullName, readableStream, readableStreamLength);
            console.log('After');
        });
    }
    read(folderName, name) {
        throw new Error('not yet implemented');
    }
    readAsBuffer(folderName, name) {
        return __awaiter(this, void 0, Promise, function* () {
            throw new Error('not yet implemented');
        });
    }
    readAsObject(folderName, name) {
        return __awaiter(this, void 0, Promise, function* () {
            throw new Error('not yet implemented');
        });
    }
}
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = AzureBlobStorage;
