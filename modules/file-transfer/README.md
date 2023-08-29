# Ignite File Transfer

This module provides a way to transfer files between the host and the target.

## Usage

The module provides the `org.apache.ignite.internal.network.file.FileTransferService` class that can be used to transfer files between the
host and the target.

### Identifier

For all files that are going to be transferred, identifiers must be provided.
The identifier is represented by the `org.apache.ignite.internal.network.file.messages.Identifier` class.
The identifier allows fetching files from the target and uploading files to the target.

### Downloading files from the target

For downloading files from the target, the target should have a `FileTransferService` instance running
and a registered `org.apache.ignite.internal.network.file.FileProvider` for the files that are to be downloaded.
The `FileProvider` is a function that accepts an `Identifier` object and returns a collection of `File` objects.

### Uploading files to the target

For uploading files to the target, the target should have a `FileTransferService` instance running
and a registered `org.apache.ignite.internal.network.file.FileConsumer` for the files that are to be uploaded.
The `FileConsumer` is a function that accepts an `Identifier` object and a collection of `File` objects.
