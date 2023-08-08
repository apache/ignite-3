# Ignite File Transfer

This module provides a way to transfer files between the host and the target.

## Usage

The module provides a `org.apache.ignite.internal.network.file.FileTransferService` class that can be used to transfer files between the
host and the target.

### Metadata 
For all files, that are going to be transferred, the metadata should be provided.
The metadata is represented by the `org.apache.ignite.internal.network.file.messages.Metadata` class. 
The metadata allows to fetch files from the target and upload files to the target.

### Downloading files from the target
For downloading files from the target, the target should have a `FileTransferService` instance running 
and registered `org.apache.ignite.internal.network.file.FileProvider` for the files that should be downloaded.
The `FileProvider` is a function that accepts a `Metadata` object and returns a collection of `File` objects.

### Uploading files to the target
For uploading files to the target, the target should have a `FileTransferService` instance running
and registered `org.apache.ignite.internal.network.file.FileConsumer` for the files that should be uploaded.
The `FileConsumer` is a function that accepts a `Metadata` object and a collection of `File` objects.
