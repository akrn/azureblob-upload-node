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
var azure_storage_1 = require('azure-storage');
class AzureBlobStorage {
    /**
     * Optional folderName parameter can be set in order to use shortcut save/read methods
     */
    constructor(connectionString, containerName, verbose) {
        this.log = verbose ? console.log.bind(console) : () => void 0;
        this.blobService = azure_storage_1.azure.createBlobService();
        this.blobStorageContainerName = containerName;
    }
    save(folderName, name, object) {
        return __awaiter(this, void 0, Promise, function* () {
            return new Promise((resolve, reject) => {
            });
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
