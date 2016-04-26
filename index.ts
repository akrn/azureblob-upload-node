"use strict";

import stream = require('stream');
import fs = require('fs');
import zlib = require('zlib');
const azure = require('azure-storage');
const promisify = require('es6-promisify');
const streamBuffers = require('stream-buffers');


interface IBlobStorage {
    save(fullBlobName: string, object: any): Promise<any>;

    read(fullBlobName: string, writableStream: stream.Writable): Promise<any>;
    readAsBuffer(fullBlobName: string): Promise<Buffer>;
    readAsObject(fullBlobName: string): Promise<Object>;

    list(prefix: string): Promise<IBlobObject[]>;

    setRetriesCount(retriesCount: number, retryInterval?: number);
}

interface IBlobObject {
    fullBlobName: string;
    properties: any; // Azure-specific properties
    metadata?: any;
}

interface IAzureBlobSaveOptions {
    streamLength?: number; // Required when saving a stream
    contentType?: string;
    compress?: boolean;
    getURL?: boolean;
    metadata?: any;
}

/* Metadata keys which will be ignored when saving a blob */
const RESERVED_METADATA_KEYS = ['type', 'compressed'];

class AzureBlobStorage implements IBlobStorage {
    blobService: any;
    blobStorageContainerName: string;
    retriesCount: number;
    retryInterval: number;

    log: (...args) => void;

    constructor(connectionString: string, containerName: string, loggerFunction?: (...args) => void) {
        this.log = loggerFunction || (() => null);

        this.blobService = azure.createBlobService(connectionString);
        this.blobStorageContainerName = containerName;
        this.retriesCount = 1;
        this.retryInterval = 500;
    }
    
    /**
     * Set a number of retries and interval between them
     */
    setRetriesCount(retriesCount: number, retryInterval?: number) {
        this.retriesCount = retriesCount;
        if (retryInterval) {
            this.retryInterval = retryInterval;
        }
    }

    async save(fullBlobName: string, object: any, options?: IAzureBlobSaveOptions): Promise<any> {
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

        let readableStream,
            readableStreamLength = 0;

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

        } else if (object instanceof Buffer) {
            this.log('Object type: buffer');

            readableStream = new stream.Readable();
            readableStream._read = () => {
                readableStream.push(object);
                readableStream.push(null);
            };

            readableStreamLength = (<Buffer>object).length;

            blobOptions.metadata['type'] = 'binary';

        } else if (typeof object === 'string') {
            this.log('Object type: local filename');
            
            // Will throw error in case file is not exists
            let fileStat = fs.statSync(object);
            readableStream = fs.createReadStream(object);
            readableStreamLength = fileStat.size;

            blobOptions.metadata['type'] = 'binary';

        } else if (object instanceof Object) {
            this.log('Object type: json');

            let stringData = JSON.stringify(object, null, 0),
                buffer = new Buffer(stringData, 'utf8');

            readableStream = new stream.Readable();
            readableStream._read = () => {
                readableStream.push(buffer);
                readableStream.push(null);
            };

            readableStreamLength = buffer.length;

            blobOptions.metadata['type'] = 'json';

        } else {
            throw new Error('Unsupported object type');
        }

        if (options && options.contentType) {
            this.log(`Setting contentType for the blob: ${options.contentType}`);

            blobOptions['contentType'] = options.contentType;
        }

        if (options && options.compress) {
            this.log('Applying compression');

            let compressedStream = new stream.PassThrough();
            readableStreamLength = await this.compressStream(readableStream, compressedStream);
            readableStream = compressedStream;

            blobOptions.metadata['compressed'] = true;
        }

        if (!readableStreamLength) {
            throw new Error('Stream length is required');
        }

        this.log(`Stream length: ${readableStreamLength}`);

        let retry = false,
            azureError = null;

        do {
            try {
                await promisify(this.blobService.createContainerIfNotExists.bind(this.blobService))(this.blobStorageContainerName, { publicAccessLevel: 'blob' });
                await promisify(this.blobService.createBlockBlobFromStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, readableStream, readableStreamLength, blobOptions);

                // No error, unset azure error and exit the loop
                retry = false;
                azureError = null;
            } catch (err) {
                // Decrease retries count and decide whether to retry operation
                this.retriesCount--;
                retry = (this.retriesCount > 0);
                azureError = err;

                this.log(`Error while saving blob: ${err}. Retries left: ${this.retriesCount}`);

                // Wait before the next retry
                if (retry) {
                    await this.timeout(this.retryInterval);
                }
            }
        } while (retry);

        if (azureError) {
            // Failed to save, throw original error
            throw azureError;
        }

        if (options && options.getURL) {
            this.log('Retrieving URL');

            let url = await this.getURL(fullBlobName);

            return url;
        }
    }

    async read(fullBlobName: string, writableStream: stream.Writable): Promise<any> {
        let blobStream = await this.readBlob(fullBlobName);
        blobStream.pipe(writableStream);
    }

    async readAsBuffer(fullBlobName: string): Promise<Buffer> {
        let passThroughStream = new stream.PassThrough();

        let blobStream = await this.readBlob(fullBlobName);

        return await this.streamToBuffer(blobStream);
    }

    async readAsObject(fullBlobName: string): Promise<Object> {
        let metadata = Object.create(null),
            blobStream = await this.readBlob(fullBlobName, metadata);

        let buffer = await this.streamToBuffer(blobStream);

        return JSON.parse(buffer.toString('utf8'));
    }

    async list(prefix: string): Promise<IBlobObject[]> {
        let listBlobsSegmentedWithPrefixAsync = promisify(this.blobService.listBlobsSegmentedWithPrefix.bind(this.blobService)),
            result,
            continuationToken = null,
            list: IBlobObject[];

        do {
            result = await listBlobsSegmentedWithPrefixAsync(this.blobStorageContainerName, prefix, continuationToken, { include: 'metadata' });

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

            // Check if we have more items to retrieve
        } while (continuationToken);

        return list;
    }

    getURL(fullBlobName: string): string {
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


    private async streamToBuffer(readableStream: stream.Stream): Promise<Buffer> {
        return new Promise<Buffer>((resolve, reject) => {
            let buffers = [];

            readableStream
                .on('readable', () => {
                    let chunk;
                    while ((chunk = (<stream.Readable>readableStream).read()) !== null) {
                        buffers.push(chunk);
                    }
                })
                .on('end', () => {
                    resolve(Buffer.concat(buffers));
                });
        });
    }

    private async compressStream(readableStream: stream.Readable, writableStream: stream.PassThrough): Promise<number> {
        return new Promise<number>((resolve, reject) => {
            let length = 0,
                counterStream = new stream.PassThrough(),
                zlibStream = zlib.createGzip();

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
    }

    private async readBlob(fullBlobName: string, metadata?: any): Promise<stream.Stream> {
        let writable,
            passThrough,
            totalData = 0;
        
        function createStreams() {
            writable = new stream.Writable();
            passThrough = new stream.PassThrough();
            totalData = 0;

            writable._write = function(chunk, _, next) {
                totalData += chunk.length;
                passThrough.push(chunk);
                next();
            };
            writable.on('finish', () => {
                passThrough.end();
            });

            return writable, passThrough;
        }

        let retry = false,
            azureError = null,
            result;

        do {
            try {
                createStreams();
                result = await this.getBlobToStream(this.blobStorageContainerName, fullBlobName, writable);

                // No error, unset azure error and exit the loop
                retry = false;
                azureError = null;
            } catch (err) {
                // Decrease retries count and decide whether to retry operation
                this.retriesCount--;
                retry = (this.retriesCount > 0);
                azureError = err;

                this.log(`Error while reading blob: ${err}. Retries left: ${this.retriesCount}`);

                // Wait before the next retry
                if (retry) {
                    await this.timeout(this.retryInterval);
                }
            }
        } while (retry);

        if (azureError) {
            // Failed to read, throw original error
            throw azureError;
        }

        if (metadata && result.metadata) {
            // In case we have metadata and supplied an object to read it there
            Object.assign(metadata, result.metadata)
        }

        if (result.metadata && result.metadata.compressed) {
            this.log('Decompressing compressed blob...');
            let decomressedPassThroughStream = new stream.PassThrough();

            passThrough.pipe(zlib.createGunzip()).pipe(decomressedPassThroughStream);

            return decomressedPassThroughStream;
        }

        return passThrough;
    }

    /**
     * Wrapper around BlobService.getBlobToStream function to make it return Promise - promisify() works incorrectly here
     */
    private async getBlobToStream(containerName: string, fullBlobName: string, writableStream: stream.Writable): Promise<any> {
        return new Promise((resolve, reject) => {
            this.blobService.getBlobToStream(containerName, fullBlobName, writableStream, (err, result, _) => {
                if (err) {
                    return reject(err);
                }

                resolve(result);
            });
        });
    }

    async timeout(ms: number): Promise<any> {
        return new Promise((resolve, reject) => {
            setTimeout(resolve, ms);
        });
    }
}

export = AzureBlobStorage;
