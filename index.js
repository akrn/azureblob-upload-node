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
/* Metadata keys which will be ignored when saving a blob */
const RESERVED_METADATA_KEYS = ['type', 'compressed'];
class AzureBlobStorage {
    constructor(connectionString, containerName, verbose) {
        this.log = verbose ? console.log.bind(console) : () => void 0;
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
        return __awaiter(this, void 0, Promise, function* () {
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
            let retry = false, azureError = null;
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
                    this.retriesCount--;
                    retry = (this.retriesCount > 0);
                    azureError = err;
                    this.log(`Error while saving blob: ${err}. Retries left: ${this.retriesCount}`);
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
        return __awaiter(this, void 0, Promise, function* () {
            let blobStream = yield this.readBlob(fullBlobName);
            blobStream.pipe(writableStream);
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
            let metadata = Object.create(null), blobStream = yield this.readBlob(fullBlobName, metadata);
            let buffer = yield this.streamToBuffer(blobStream);
            return JSON.parse(buffer.toString('utf8'));
        });
    }
    list(prefix) {
        return __awaiter(this, void 0, Promise, function* () {
            let listBlobsSegmentedWithPrefixAsync = promisify(this.blobService.listBlobsSegmentedWithPrefix.bind(this.blobService)), result, continuationToken = null, list;
            do {
                result = yield listBlobsSegmentedWithPrefixAsync(this.blobStorageContainerName, prefix, continuationToken, { include: 'metadata' });
                list = result[0].entries.map((entry) => {
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
                continuationToken = result[0].continuationToken;
            } while (continuationToken);
            return list;
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
    readBlob(fullBlobName, metadata) {
        return __awaiter(this, void 0, Promise, function* () {
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
                return writable, passThrough;
            }
            let retry = false, azureError = null, result;
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
                    this.retriesCount--;
                    retry = (this.retriesCount > 0);
                    azureError = err;
                    this.log(`Error while reading blob: ${err}. Retries left: ${this.retriesCount}`);
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
        return __awaiter(this, void 0, Promise, function* () {
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
        return __awaiter(this, void 0, Promise, function* () {
            return new Promise((resolve, reject) => {
                setTimeout(resolve, ms);
            });
        });
    }
}
module.exports = AzureBlobStorage;
