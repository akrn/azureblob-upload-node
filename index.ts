/// <reference path='typings/node/node.d.ts' />
"use strict";

const azure = require('azure-storage');


interface IBlobStorage {
    save(folderName: string, name: string, object: any): Promise<boolean>;

    read(folderName: string, name: string): NodeJS.ReadableStream;
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

        this.blobService = azure.createBlobService();
        this.blobStorageContainerName = containerName;
    }

    async save(folderName: string, name: string, object: any): Promise<boolean> {
        return new Promise<boolean>((resolve, reject) => {
            
        });
    }

    read(folderName: string, name: string): NodeJS.ReadableStream {
        throw new Error('not yet implemented');
    }

    async readAsBuffer(folderName: string, name: string): Promise<Buffer> {
        throw new Error('not yet implemented');
    }

    async readAsObject(folderName: string, name: string): Promise<Object> {
        throw new Error('not yet implemented');
    }
}
