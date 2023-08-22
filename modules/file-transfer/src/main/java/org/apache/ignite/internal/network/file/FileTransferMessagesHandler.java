/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.file.exception.FileTransferException;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Handler for file transfer messages.
 */
class FileTransferMessagesHandler implements TransferredFilesCollector {
    private final int filesCount;

    private final Path dir;

    private final AtomicInteger filesComplete = new AtomicInteger(0);

    private final CompletableFuture<List<Path>> result = new CompletableFuture<>();

    private final Map<String, ChunkedFileWriter> fileNameToWriter = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param dir Directory to write files to.
     */
    FileTransferMessagesHandler(List<FileHeader> headers, Path dir) {
        this.filesCount = headers.size();
        this.dir = dir;
        headers.forEach(this::handleFileHeader);
    }

    /**
     * Handles file header. Creates file writer if it doesn't exist yet. Closes file writer if all chunks are received.
     *
     * @param header File header.
     */
    private void handleFileHeader(FileHeader header) {
        File file = createFile(header.name());
        if (header.length() == 0) {
            filesComplete.incrementAndGet();
        } else {
            fileNameToWriter.put(header.name(), createFileWriter(file, header.length()));
        }
    }

    /**
     * Handles file chunk. Creates file writer if it doesn't exist yet. Closes file writer if all chunks are received.
     *
     * @param fileChunk File chunk.
     */
    void handleFileChunk(FileChunkMessage fileChunk) {
        if (result.isDone()) {
            throw new IllegalStateException("Received chunked file after result is already done");
        }

        try {
            ChunkedFileWriter writer = fileNameToWriter.get(fileChunk.fileName());

            assert writer != null : "Received file chunk for unknown file: " + fileChunk.fileName();

            if (writer.write(fileChunk)) {
                filesComplete.incrementAndGet();
                completeIfAllFilesFinished();
            }
        } catch (IOException e) {
            handleFileTransferError(e);
        }
    }

    /**
     * Handles file transfer error. Closes all file writers and completes result exceptionally.
     *
     * @param error Error.
     */
    void handleFileTransferError(Throwable error) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file transfer error after result is already done");
        }

        result.completeExceptionally(error);
        closeAllWriters();
    }

    /**
     * Completes result if all files are finished.
     */
    private void completeIfAllFilesFinished() throws IOException {
        if (filesCount == filesComplete.get()) {
            try (Stream<Path> stream = java.nio.file.Files.list(dir)) {
                result.complete(stream.collect(Collectors.toList()));
            }
        }
    }

    /**
     * Returns result future.
     *
     * @return Result future.
     */
    @Override
    public CompletableFuture<List<Path>> collectedFiles() {
        return result;
    }

    /**
     * Close all writers.
     */
    private void closeAllWriters() {
        try {
            IgniteUtils.closeAll(fileNameToWriter.values());
        } catch (Exception e) {
            throw new FileTransferException("Failed to close file writers", e);
        }
    }

    /**
     * Creates file writer with known file size.
     *
     * @param file File to write to.
     * @param expectedFileSize Expected file size.
     * @return File writer.
     */
    private ChunkedFileWriter createFileWriter(File file, long expectedFileSize) {
        try {
            return ChunkedFileWriter.open(file, expectedFileSize);
        } catch (IOException e) {
            handleFileTransferError(e);
            throw new FileTransferException("Failed to open file writer", e);
        }
    }

    private File createFile(String fileName) {
        try {
            return java.nio.file.Files.createFile(dir.resolve(fileName)).toFile();
        } catch (IOException e) {
            handleFileTransferError(e);
            throw new FileTransferException("Failed to create file", e);
        }
    }
}
