/// <reference path='typings/node/node.d.ts' />
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

    list(folderName: string): Promise<IBlobObject[]>;
}

interface IBlobObject {
    fullBlobName: string;
    properties: any; // Azure-specific properties
}

interface IAzureBlobSaveOptions {
    streamLength?: number; // Required when saving a stream
    contentType?: string;
    compress?: boolean;
    getURL?: boolean;
}

export default class AzureBlobStorage implements IBlobStorage {
    blobService: any;
    blobStorageContainerName: string;

    log: (...args) => void;

    constructor(connectionString: string, containerName: string, verbose?: boolean) {
        this.log = verbose ? console.log.bind(console) : () => void 0;

        this.blobService = azure.createBlobService(connectionString);
        this.blobStorageContainerName = containerName;
    }

    async save(fullBlobName: string, object: any, options?: IAzureBlobSaveOptions): Promise<any> {
        let blobOptions = {
                metadata: {}
            };

        let readableStream,
            readableStreamLength;

        if (object instanceof stream.Readable) {
            this.log('Object type: stream');

            if (!options || !options.streamLength) {
                throw new Error('Stream length is required');
            }

            readableStream = object;
            readableStreamLength = options.streamLength;

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

        this.log(`Stream length: ${readableStreamLength}`);

        await promisify(this.blobService.createContainerIfNotExists.bind(this.blobService))(this.blobStorageContainerName, { publicAccessLevel: 'blob' });
        await promisify(this.blobService.createBlockBlobFromStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, readableStream, readableStreamLength, blobOptions);

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

        if (metadata.type !== 'json') {
            throw new Error('The requested blob can\'t be downloaded as JSON object');
        }

        let buffer = await this.streamToBuffer(blobStream);

        return JSON.parse(buffer.toString('utf8'));
    }

    async list(folderName: string): Promise<IBlobObject[]> {
        let listBlobsSegmentedAsync = promisify(this.blobService.listBlobsSegmented.bind(this.blobService)),
            result,
            continuationToken = null,
            list: IBlobObject[];

        do {
            result = await listBlobsSegmentedAsync(this.blobStorageContainerName, continuationToken);

            list = result[0].entries.map((entry) => {
                return {
                    fullBlobName: entry.name,
                    properties: entry.properties
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
        return new Promise<stream.Stream>((resolve, reject) => {
            let writable = new stream.Writable(),
                passThrough = new stream.PassThrough();

            let total = 0;
            writable._write = function(chunk, _, next) {
                total += chunk.length;
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

                if (metadata && result.metadata) {
                    // In case we have metadata and supplied an object to read it there
                    Object.assign(metadata, result.metadata)
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
    }
}
