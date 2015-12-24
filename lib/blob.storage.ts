'use strict';

var azure = require('azure-storage');
var config = require('../config/environment');
var stream = require('stream');
var convertJsonToBuffer = require('../utils/utils').convertJsonToBuffer;

export interface IBlobStorage {
    save(folderName: string, name: string, input: any, getUrl: boolean): Promise<string>;
    read(folderName: string, name: string, writeStream: any): any;
    //list(name: string): any;
}

export function createBlobStorage(storageProvider: string, containerName: string) {
    return new AzureBlobStorage(containerName);
}

export class AzureBlobStorage implements IBlobStorage {
    blobService: any;
    blobStorageContainerName: string;

    constructor(containerName: string) {
        this.blobService = azure.createBlobService();
        this.blobStorageContainerName = containerName;
    }

    //Input, readStream or Object
    async save(folderName: string, name: string, input: any, getUrl: boolean): Promise<string> {
        return new Promise<string>((resolve, reject) => {
            let id = folderName + ':' + name;
            let len = 99 * 1024 * 1024; // http://www.carbonatethis.com/streaming-files-to-azure-blob-storage-with-node-js/
            let uploadOptions = {
                blockIdPrefix : 'media'
            };

            let readStream;
            if (typeof input.read == 'function') {
                readStream = input;
            } else {
                readStream = new stream.PassThrough();
                readStream.end(convertJsonToBuffer(input));
            }

            this.blobService.createBlockBlobFromStream(this.blobStorageContainerName, id, readStream, len, uploadOptions, (err) => {
                if (err) {
                    reject(err);
                } else {

                    if (getUrl) {
                        let startDate = new Date();
                        let expiryDate = new Date(startDate);

                        expiryDate.setMinutes(startDate.getMinutes() + 100);
                        startDate.setMinutes(startDate.getMinutes() - 100);

                        let sharedAccessPolicy = {
                            AccessPolicy: {
                                ermissions: azure.BlobUtilities.SharedAccessPermissions.READ,
                                Start: startDate,
                                Expiry: expiryDate
                            },
                        };

                        let token = this.blobService.generateSharedAccessSignature(this.blobStorageContainerName, id, sharedAccessPolicy);
                        let sasUrl = this.blobService.getUrl(this.blobStorageContainerName, id, token);
                        resolve(sasUrl);
                    } else {
                        resolve(id);
                    }
                }
            });
        });
    }

    read(folderName: string, name: string, writeStream: any) {
        return new Promise<void>((resolve, reject) => {
            let id = folderName + ':' + name;

            this.blobService.getBlobToStream(this.blobStorageContainerName, id, writeStream, (err, result, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            })
        });
    }
}
