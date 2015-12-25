/// <reference path='typings/node/node.d.ts' />
"use strict";

import stream = require('stream');
const azure = require('azure-storage');
const promisify = require('es6-promisify');

const FOLDER_SEPARATOR = ':';


interface IBlobStorage {
    save(folderName: string, name: string, object: any): Promise<boolean>;

    read(folderName: string, name: string): stream.Readable;
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

    async save(folderName: string, name: string, object: any): Promise<any> {
        let readableStream,
            readableStreamLength;

        if (object instanceof stream.Readable) {
            throw new Error('not yet implemented');
        } else if (object instanceof Buffer) {
            throw new Error('not yet implemented');
        } else if (typeof object === 'string') {
            throw new Error('not yet implemented');
        } else if (object instanceof Object) {
            let stringData = JSON.stringify(object, null, 0);

            readableStream = new stream.Readable();
            readableStream._read = ()=>{};
            readableStream.push(stringData);
            readableStreamLength = stringData.length;
        } else {
            throw new Error('Unsupported object type');
        }

        let fullName = [folderName, name].join(FOLDER_SEPARATOR);

        let result = await promisify(this.blobService.createContainerIfNotExists.bind(this.blobService))(this.blobStorageContainerName, {
            publicAccessLevel: 'blob'
        });

        this.log(`createContainerIfNotExists(${this.blobStorageContainerName}) result:`, result);

        await promisify(this.blobService.createBlockBlobFromStream.bind(this.blobService))(this.blobStorageContainerName, fullName, readableStream, readableStreamLength);
    }

    read(folderName: string, name: string): stream.Readable {
        throw new Error('not yet implemented');
    }

    async readAsBuffer(folderName: string, name: string): Promise<Buffer> {
        throw new Error('not yet implemented');
    }

    async readAsObject(folderName: string, name: string): Promise<Object> {
        throw new Error('not yet implemented');
    }
}
