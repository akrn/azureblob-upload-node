/// <reference path='typings/node/node.d.ts' />
"use strict";

import stream = require('stream');
import fs = require('fs');
const azure = require('azure-storage');
const promisify = require('es6-promisify');

const FOLDER_SEPARATOR = ':';


interface IBlobStorage {
    save(folderName: string, name: string, object: any): Promise<any>;

    read(folderName: string, name: string, writableStream: stream.Writable): Promise<any>;
    readAsBuffer(folderName: string, name: string): Promise<Buffer>;
    readAsObject(folderName: string, name: string): Promise<Object>;
}

export default class AzureBlobStorage implements IBlobStorage {
    blobService: any;
    blobStorageContainerName: string;
    folderName: string;

    log: (...args) => void;

    /**
     * Optional folderName parameter can be set in order to use shortcut save/read methods
     */
    constructor(connectionString: string, containerName: string, verbose?: boolean) {
        this.log = verbose ? console.log.bind(console) : () => void 0;

        this.blobService = azure.createBlobService(connectionString);
        this.blobStorageContainerName = containerName;
    }

    async save(folderName: string, name: string, object: any, options?: any): Promise<any> {
        let fullBlobName = [folderName, name].join(FOLDER_SEPARATOR),
            blobOptions = {
                metadata: {}
            };

        let readableStream,
            readableStreamLength;

        if (object instanceof stream.Readable) {
            this.log('Object type: stream');

            if (!options || !options['length']) {
                throw new Error('Stream length is required');
            }

            readableStream = object;
            readableStreamLength = +options['length'];

            blobOptions.metadata['type'] = 'binary';

        } else if (object instanceof Buffer) {
            this.log('Object type: buffer');

            readableStream = new stream.Readable();
            readableStream._read = () => { };
            readableStream.push(object);
            readableStream.push(null);
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

            let stringData = JSON.stringify(object, null, 0);

            readableStream = new stream.Readable();
            readableStream._read = () => {};
            readableStream.push(stringData);
            readableStream.push(null);
            readableStreamLength = stringData.length;

            blobOptions.metadata['type'] = 'json';

        } else {
            throw new Error('Unsupported object type');
        }

        if (options && options['contentType']) {
            this.log(`Setting contentType for the blob: ${options['contentType']}`);

            blobOptions['contentType'] = options['contentType'];
        }

        this.log(`Stream length: ${readableStreamLength}`);

        await promisify(this.blobService.createContainerIfNotExists.bind(this.blobService))(this.blobStorageContainerName, { publicAccessLevel: 'blob' });
        await promisify(this.blobService.createBlockBlobFromStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, readableStream, readableStreamLength, blobOptions);
    }

    async read(folderName: string, name: string, writableStream: stream.Writable): Promise<any> {
        let fullBlobName = [folderName, name].join(FOLDER_SEPARATOR);

        await promisify(this.blobService.getBlobToStream.bind(this.blobService))(this.blobStorageContainerName, fullBlobName, writableStream);
    }

    async readAsBuffer(folderName: string, name: string): Promise<Buffer> {
        let fullBlobName = [folderName, name].join(FOLDER_SEPARATOR);

        let passThroughStream = new stream.PassThrough();
        await this.blobService.getBlobToStream(this.blobStorageContainerName, fullBlobName, passThroughStream, (e) => { if (e) throw e; });

        return await this.streamToBuffer(passThroughStream);
    }

    async readAsObject(folderName: string, name: string): Promise<Object> {
        let fullBlobName = [folderName, name].join(FOLDER_SEPARATOR);

        let result = await promisify(this.blobService.getBlobToText.bind(this.blobService))(this.blobStorageContainerName, fullBlobName),
            text = result[0],
            metadata = result[1].metadata;

        if (metadata.type !== 'json') {
            throw new Error('The requested blob can\'t be downloaded as JSON object');
        }

        return JSON.parse(text);
    }

    private async streamToBuffer(readableStream: stream.Stream): Promise<Buffer> {
        return new Promise<Buffer>((resolve, reject) => {
            let buffers = [];
            readableStream
                .on('data', (data) => buffers.push(data))
                .on('end', () => resolve(Buffer.concat(buffers)))
        });
    }
}
