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
var zlib = require('zlib');
const azure = require('azure-storage');
const promisify = require('es6-promisify');
const streamBuffers = require('stream-buffers');
class AzureBlobStorage {
    constructor(connectionString, containerName, verbose) {
        this.log = verbose ? console.log.bind(console) : () => void 0;
        this.blobService = azure.createBlobService(connectionString);
        this.blobStorageContainerName = containerName;
    }
    save(fullBlobName, object, options) {
        return __awaiter(this, void 0, Promise, function* () {
            let blobOptions = {
                metadata: {}
            };
            let readableStream, readableStreamLength;
            if (object instanceof stream.Readable) {
                this.log('Object type: stream');
                if (!options || !options.streamLength) {
                    throw new Error('Stream length is required');
                }
                readableStream = object;
                readableStreamLength = options.streamLength;
                blobOptions.metadata['type'] = 'binary';
            }
            else if (object instanceof Buffer) {
                this.log('Object type: buffer');
                readableStream = new stream.Readable();
                readableStream._read = () => {
                    readableStream.push(object);
                    readableStream.push(null);
                };
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
            if (options && options.contentType) {
                this.log(`Setting contentType for the blob: ${options.contentType}`);
                blobOptions['contentType'] = options.contentType;
            }
            if (options && options.compress) {
                this.log('Applying compression');
                let compressedStream = new stream.PassThrough();
                readableStreamLength = yield this.compressStream(readableStream, compressedStream);
                readableStream = compressedStream;
                blobOptions.metadata['compressed'] = true;
            }
            this.log(`Stream length: ${readableStreamLength}`);
            yield promisify(this.blobService.createContainerIfNotExists.bind(this.blobService))(this.blobStorageContainerName, { publicAccessLevel: 'blob' });
            yield promisify(this.blobService.createBlockBlobFromStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, readableStream, readableStreamLength, blobOptions);
            // this.blobService.createBlockBlobFromStream(this.blobStorageContainerName, fullBlobName, readableStream, readableStreamLength, blobOptions, (err, resp1, resp2) => {
            // console.log(err, resp1, resp2);
            // });
        });
    }
    read(fullBlobName, writableStream) {
        return __awaiter(this, void 0, Promise, function* () {
            yield promisify(this.blobService.getBlobToStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, writableStream);
        });
    }
    readAsBuffer(fullBlobName) {
        return __awaiter(this, void 0, Promise, function* () {
            let passThroughStream = new stream.PassThrough();
            let blobStream = yield this.readBlob(fullBlobName);
            return yield this.streamToBuffer(blobStream);
        });
    }
    readAsObject(fullBlobName) {
        return __awaiter(this, void 0, Promise, function* () {
            let result = yield promisify(this.blobService.getBlobToText.bind(this.blobService))(this.blobStorageContainerName, fullBlobName), text = result[0], metadata = result[1].metadata;
            if (metadata.type !== 'json') {
                throw new Error('The requested blob can\'t be downloaded as JSON object');
            }
            return JSON.parse(text);
        });
    }
    // Private methods
    streamToBuffer(readableStream) {
        return __awaiter(this, void 0, Promise, function* () {
            return new Promise((resolve, reject) => {
                let buffers = [];
                readableStream
                    .on('readable', () => {
                    let chunk;
                    while ((chunk = readableStream.read()) !== null) {
                        buffers.push(chunk);
                    }
                })
                    .on('end', () => {
                    resolve(Buffer.concat(buffers));
                });
            });
        });
    }
    compressStream(readableStream, writableStream) {
        return __awaiter(this, void 0, Promise, function* () {
            return new Promise((resolve, reject) => {
                let length = 0, counterStream = new stream.PassThrough(), zlibStream = zlib.createGzip();
                counterStream
                    .on('readable', () => {
                    let chunk;
                    while ((chunk = counterStream.read()) !== null) {
                        length += chunk.length;
                        writableStream.write(chunk);
                    }
                })
                    .on('end', () => {
                    writableStream.end();
                    resolve(length);
                });
                zlibStream.pipe(counterStream);
                readableStream
                    .on('readable', () => {
                    let chunk;
                    while ((chunk = readableStream.read()) !== null) {
                        zlibStream.write(chunk);
                    }
                })
                    .on('end', () => {
                    zlibStream.end();
                });
            });
        });
    }
    readBlob(fullBlobName) {
        return __awaiter(this, void 0, Promise, function* () {
            return new Promise((resolve, reject) => {
                let writable = new stream.Writable(), passThrough = new stream.PassThrough();
                writable._write = function (chunk, _, next) {
                    passThrough.push(chunk);
                    next();
                };
                writable.on('finish', () => {
                    passThrough.end();
                });
                this.blobService.getBlobToStream(this.blobStorageContainerName, fullBlobName, writable, (err, result, _) => {
                    if (err) {
                        return reject(err);
                    }
                    if (result.metadata && result.metadata.compressed) {
                        this.log('Decompressing compressed blob...');
                        let decomressedPassThroughStream = new stream.PassThrough();
                        passThrough.pipe(zlib.createGunzip()).pipe(decomressedPassThroughStream);
                        return resolve(decomressedPassThroughStream);
                    }
                    return resolve(passThrough);
                });
            });
        });
    }
}
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = AzureBlobStorage;
