# azureblob-upload-node
Upload various data to azure blobs from nodejs. Library is written in TypeScript 1.8 with use of async/await functions. Compilation target is set to ES6 so at least node v4 is required.

## Testing
```
npm test
```
AZURE_STORAGE_CONNECTION_STRING environment variable should be set in order to allow tests to connect Azure.
Tests are written in TypeScript as well. They got precompiled before each run.
