"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
const stream = require("stream");
const fs = require("fs");
const zlib = require("zlib");
const azure = require('azure-storage');
const promisify = require('es6-promisify');
const streamBuffers = require('stream-buffers');
/* Metadata keys which will be ignored when saving a blob */
const RESERVED_METADATA_KEYS = ['type', 'compressed'];
class AzureBlobStorage {
    constructor(connectionString, containerName, loggerFunction) {
        this.log = loggerFunction || (() => null);
        this.blobService = azure.createBlobService(connectionString);
        this.blobStorageContainerName = containerName;
        this.retriesCount = 1;
        this.retryInterval = 500;
    }
    /**
     * Set a number of retries and interval between them
     */
    setRetriesCount(retriesCount, retryInterval) {
        this.retriesCount = retriesCount;
        if (retryInterval) {
            this.retryInterval = retryInterval;
        }
    }
    save(fullBlobName, object, options) {
        return __awaiter(this, void 0, void 0, function* () {
            let blobOptions = {
                metadata: {}
            };
            if (options && options.metadata) {
                for (let key in options.metadata) {
                    if (RESERVED_METADATA_KEYS.indexOf(key) !== -1) {
                        this.log(`Skipping reserved metadata key: ${key}`);
                        continue;
                    }
                    blobOptions.metadata[key] = options.metadata[key];
                }
            }
            let readableStream, readableStreamLength = 0;
            if (object instanceof stream.Readable) {
                this.log('Object type: stream');
                /* not needed if compress and expensive to get if we want to use stream
                if (!options || !options.streamLength) {
                    throw new Error('Stream length is required');
                }*/
                readableStream = object;
                if (options) {
                    readableStreamLength = options.streamLength;
                }
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
                let stringData = JSON.stringify(object, null, 0), buffer = new Buffer(stringData, 'utf8');
                readableStream = new stream.Readable();
                readableStream._read = () => {
                    readableStream.push(buffer);
                    readableStream.push(null);
                };
                readableStreamLength = buffer.length;
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
            if (!readableStreamLength) {
                throw new Error('Stream length is required');
            }
            this.log(`Stream length: ${readableStreamLength}`);
            let retry = false, azureError = null, retriesCount = this.retriesCount;
            do {
                try {
                    yield promisify(this.blobService.createContainerIfNotExists.bind(this.blobService))(this.blobStorageContainerName, { publicAccessLevel: 'blob' });
                    yield promisify(this.blobService.createBlockBlobFromStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, readableStream, readableStreamLength, blobOptions);
                    // No error, unset azure error and exit the loop
                    retry = false;
                    azureError = null;
                }
                catch (err) {
                    // Decrease retries count and decide whether to retry operation
                    retriesCount--;
                    retry = (retriesCount > 0);
                    azureError = err;
                    this.log(`Error while saving blob: ${err}. Retries left: ${retriesCount}`);
                    // Wait before the next retry
                    if (retry) {
                        yield this.timeout(this.retryInterval);
                    }
                }
            } while (retry);
            if (azureError) {
                // Failed to save, throw original error
                throw azureError;
            }
            if (options && options.getURL) {
                this.log('Retrieving URL');
                let url = yield this.getURL(fullBlobName);
                return url;
            }
        });
    }
    read(fullBlobName, writableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            let blobStream = yield this.readBlob(fullBlobName);
            blobStream.pipe(writableStream);
        });
    }
    readAsBuffer(fullBlobName) {
        return __awaiter(this, void 0, void 0, function* () {
            let passThroughStream = new stream.PassThrough();
            let blobStream = yield this.readBlob(fullBlobName);
            return yield this.streamToBuffer(blobStream);
        });
    }
    readAsObject(fullBlobName) {
        return __awaiter(this, void 0, void 0, function* () {
            let metadata = Object.create(null), blobStream = yield this.readBlob(fullBlobName, metadata);
            let buffer = yield this.streamToBuffer(blobStream);
            return JSON.parse(buffer.toString('utf8'));
        });
    }
    list(prefix) {
        return __awaiter(this, void 0, void 0, function* () {
            let list = [], blobIterator = this.iterator(prefix), item;
            while (item = yield blobIterator.next()) {
                list.push(item);
            }
            return list;
        });
    }
    iterator(prefix) {
        let continuationToken = null;
        let blobIterator = new IBlobIterator((cb) => {
            this.blobService.listBlobsSegmentedWithPrefix(this.blobStorageContainerName, prefix, continuationToken, { include: 'metadata' }, (err, result) => {
                if (err) {
                    return cb(err, [], false);
                }
                let list = result.entries.map(entry => {
                    // Remove reserved metadata keys
                    let metadata = {};
                    for (let key in entry.metadata) {
                        if (RESERVED_METADATA_KEYS.indexOf(key) === -1) {
                            metadata[key] = entry.metadata[key];
                        }
                    }
                    return {
                        fullBlobName: entry.name,
                        properties: entry.properties,
                        metadata: metadata
                    };
                });
                continuationToken = result.continuationToken;
                cb(null, list, !!continuationToken);
            });
        });
        return blobIterator;
    }
    delete(fullBlobName) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                this.blobService.deleteBlobIfExists(this.blobStorageContainerName, fullBlobName, (err, result) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(result);
                });
            });
        });
    }
    getURL(fullBlobName) {
        let expiration = new Date();
        expiration.setUTCFullYear(2050);
        let sharedAccessPermissions = {
            AccessPolicy: {
                Permissions: azure.BlobUtilities.SharedAccessPermissions.READ,
                Expiry: expiration
            }
        };
        let token = this.blobService.generateSharedAccessSignature(this.blobStorageContainerName, fullBlobName, sharedAccessPermissions);
        return this.blobService.getUrl(this.blobStorageContainerName, fullBlobName, token);
    }
    // Private methods
    streamToBuffer(readableStream) {
        return __awaiter(this, void 0, void 0, function* () {
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
        return __awaiter(this, void 0, void 0, function* () {
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
    readBlob(fullBlobName, metadata) {
        return __awaiter(this, void 0, void 0, function* () {
            let writable, passThrough, totalData = 0;
            function createStreams() {
                writable = new stream.Writable();
                passThrough = new stream.PassThrough();
                totalData = 0;
                writable._write = function (chunk, _, next) {
                    totalData += chunk.length;
                    passThrough.push(chunk);
                    next();
                };
                writable.on('finish', () => {
                    passThrough.end();
                });
            }
            let retry = false, azureError = null, result, retriesCount = this.retriesCount;
            do {
                try {
                    createStreams();
                    result = yield this.getBlobToStream(this.blobStorageContainerName, fullBlobName, writable);
                    // No error, unset azure error and exit the loop
                    retry = false;
                    azureError = null;
                }
                catch (err) {
                    // Decrease retries count and decide whether to retry operation
                    retriesCount--;
                    retry = (retriesCount > 0);
                    azureError = err;
                    this.log(`Error while reading blob: ${err}. Retries left: ${retriesCount}`);
                    // Wait before the next retry
                    if (retry) {
                        yield this.timeout(this.retryInterval);
                    }
                }
            } while (retry);
            if (azureError) {
                // Failed to read, throw original error
                throw azureError;
            }
            if (metadata && result.metadata) {
                // In case we have metadata and supplied an object to read it there
                Object.assign(metadata, result.metadata);
            }
            if (result.metadata && result.metadata.compressed) {
                this.log('Decompressing compressed blob...');
                let decomressedPassThroughStream = new stream.PassThrough();
                passThrough.pipe(zlib.createGunzip()).pipe(decomressedPassThroughStream);
                return decomressedPassThroughStream;
            }
            return passThrough;
        });
    }
    /**
     * Wrapper around BlobService.getBlobToStream function to make it return Promise - promisify() works incorrectly here
     */
    getBlobToStream(containerName, fullBlobName, writableStream) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                this.blobService.getBlobToStream(containerName, fullBlobName, writableStream, (err, result, _) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(result);
                });
            });
        });
    }
    timeout(ms) {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                setTimeout(resolve, ms);
            });
        });
    }
}
class IBlobIterator {
    constructor(requestFunction) {
        this.items = [];
        this.stop = false;
        this.requestFunction = requestFunction;
    }
    next() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.items.length && !this.stop) {
                yield this.request();
            }
            if (this.items.length) {
                return this.items.shift();
            }
            return null;
        });
    }
    request() {
        return __awaiter(this, void 0, void 0, function* () {
            return new Promise((resolve, reject) => {
                this.requestFunction((err, data, more) => {
                    if (err) {
                        return reject(err);
                    }
                    if (!more) {
                        this.stop = true;
                    }
                    this.items = this.items.concat(data);
                    resolve();
                });
            });
        });
    }
}
module.exports = AzureBlobStorage;
