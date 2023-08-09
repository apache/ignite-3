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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.network.file.messages.FileChunkMessage;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Handler for file transfer messages.
 */
class FileTransferMessagesHandler {
    private static final int UNKNOWN_FILES_COUNT = -1;

    private final Path dir;
    private final AtomicInteger filesCount = new AtomicInteger(UNKNOWN_FILES_COUNT);
    private final AtomicInteger filesFinished = new AtomicInteger(0);
    private final CompletableFuture<List<Path>> result = new CompletableFuture<>();
    private final Map<String, ChunkedFileWriter> fileNameToWriter = new ConcurrentHashMap<>();
    private final Map<String, Lock> fileNameToLock = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param dir Directory to write files to.
     */
    FileTransferMessagesHandler(Path dir) {
        this.dir = dir;
    }

    /**
     * Handles file headers. Should be called only once. Updates files count. Creates file writers.
     *
     * @param headers File headers.
     */
    void handleFileHeaders(List<FileHeader> headers) {
        if (!filesCount.compareAndSet(UNKNOWN_FILES_COUNT, headers.size())) {
            throw new IllegalStateException("Received file headers after files count is already set");
        }

        headers.forEach(this::handleFileHeader);
    }

    /**
     * Acquires lock for file name and handles file header.
     *
     * @param header File header.
     */
    private void handleFileHeader(FileHeader header) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file header after result is already done");
        }
        doInLock(header.name(), () -> handleFileHeader0(header));
    }

    /**
     * Handles file header. Creates file writer if it doesn't exist yet. Closes file writer if all chunks are received.
     *
     * @param header File header.
     */
    private void handleFileHeader0(FileHeader header) {
        ChunkedFileWriter writer = fileNameToWriter.compute(header.name(), (k, v) -> {
            if (v == null) {
                return writer(header.name(), header.length());
            } else {
                v.fileSize(header.length());
                return v;
            }
        });

        try {
            if (writer.isFinished()) {
                writer.close();
                filesFinished.incrementAndGet();

                completeIfAllFilesFinished();
            }
        } catch (IOException e) {
            handleFileTransferError(e);
        }
    }

    /**
     * Acquires lock for file name and handles file chunk.
     *
     * @param fileChunk File chunk.
     */
    void handleFileChunk(FileChunkMessage fileChunk) {
        if (result.isDone()) {
            throw new IllegalStateException("Received chunked file after result is already done");
        }
        doInLock(fileChunk.fileName(), () -> handleFileChunk0(fileChunk));
    }

    /**
     * Handles file chunk. Creates file writer if it doesn't exist yet. Closes file writer if all chunks are received.
     *
     * @param fileChunk File chunk.
     */
    private void handleFileChunk0(FileChunkMessage fileChunk) {
        try {
            ChunkedFileWriter writer = fileNameToWriter.computeIfAbsent(fileChunk.fileName(), this::writer);

            writer.write(fileChunk);

            if (writer.isFinished()) {
                writer.close();
                fileNameToWriter.remove(fileChunk.fileName());
                filesFinished.incrementAndGet();

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
     * Acquires lock for file name and executes runnable.
     *
     * @param fileName File name.
     * @param runnable Runnable.
     */
    private void doInLock(String fileName, Runnable runnable) {
        Lock lock = fileNameToLock.computeIfAbsent(fileName, k -> new ReentrantLock());
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Completes result if all files are finished.
     */
    private void completeIfAllFilesFinished() throws IOException {
        if (filesFinished.get() == filesCount.get()) {
            try (Stream<Path> stream = Files.list(dir)) {
                result.complete(stream.collect(Collectors.toList()));
            }
        }
    }

    /**
     * Returns result future.
     *
     * @return Result future.
     */
    CompletableFuture<List<Path>> result() {
        return result;
    }

    /**
     * Close all writers.
     */
    private void closeAllWriters() {
        try {
            IgniteUtils.closeAllManually(fileNameToWriter.values().stream());
        } catch (Exception e) {
            throw new IgniteInternalException(e);
        }
    }

    /**
     * Creates file writer with unknown file size.
     *
     * @param fileName File name.
     * @return File writer.
     */
    private ChunkedFileWriter writer(String fileName) {
        // Set -1 as file size to indicate that file size is unknown and will be set later.
        return writer(fileName, -1);
    }

    /**
     * Creates file writer with known file size.
     *
     * @param fileName File name.
     * @param fileSize File size.
     * @return File writer.
     */
    private ChunkedFileWriter writer(String fileName, long fileSize) {
        try {
            return ChunkedFileWriter.open(dir.resolve(fileName), fileSize);
        } catch (IOException e) {
            handleFileTransferError(e);
            throw new IgniteInternalException(e);
        }
    }
}
