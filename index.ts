/// <reference path='typings/node/node.d.ts' />
"use strict";

const azure = require('azure-storage');


export interface IBlobStorage {
    save(folderName: string, name: string, object: any): Promise<boolean>;

    read(folderName: string, name: string): NodeJS.ReadableStream;
    readAsBuffer(folderName: string, name: string): Promise<Buffer>;
    readAsObject(folderName: string, name: string): Promise<Object>;
}

export default class AzureBlobStorage implements IBlobStorage {
    blobService: any;
    blobStorageContainerName: string;
    folderName: string;

    /**
     * Optional folderName parameter can be set in order to use shortcut save/read methods
     */
    constructor(connectionString: string, containerName: string) {
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