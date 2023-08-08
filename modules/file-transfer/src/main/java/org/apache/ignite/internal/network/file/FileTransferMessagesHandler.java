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
    private final CompletableFuture<List<File>> result = new CompletableFuture<>();
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

    void handleFileHeaders(List<FileHeader> headers) {
        if (!filesCount.compareAndSet(UNKNOWN_FILES_COUNT, headers.size())) {
            throw new IllegalStateException("Received file headers after files count is already set");
        }

        headers.forEach(this::handleFileHeader);
    }

    private void handleFileHeader(FileHeader header) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file header after result is already done");
        }
        doInLock(header.name(), () -> handleFileHeader0(header));
    }

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

    void handleFileChunk(FileChunkMessage fileChunk) {
        if (result.isDone()) {
            throw new IllegalStateException("Received chunked file after result is already done");
        }
        doInLock(fileChunk.fileName(), () -> handleFileChunk0(fileChunk));
    }

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

    void handleFileTransferError(Throwable error) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file transfer error after result is already done");
        }
        result.completeExceptionally(error);
        closeAllWriters();
    }

    private void doInLock(String fileName, Runnable runnable) {
        Lock lock = fileNameToLock.computeIfAbsent(fileName, k -> new ReentrantLock());
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    private void completeIfAllFilesFinished() throws IOException {
        if (filesFinished.get() == filesCount.get()) {
            try (Stream<Path> stream = Files.list(dir)) {
                List<File> files = stream.map(Path::toFile).collect(Collectors.toList());
                result.complete(files);
            }
        }
    }

    CompletableFuture<List<File>> result() {
        return result;
    }

    private void closeAllWriters() {
        try {
            IgniteUtils.closeAllManually(fileNameToWriter.values().stream());
        } catch (Exception e) {
            throw new IgniteInternalException(e);
        }
    }

    private ChunkedFileWriter writer(String fileName) {
        // set -1 as file size to indicate that file size is unknown and will be set later
        return writer(fileName, -1);
    }

    private ChunkedFileWriter writer(String fileName, long fileSize) {
        try {
            return ChunkedFileWriter.open(dir.resolve(fileName), fileSize);
        } catch (IOException e) {
            handleFileTransferError(e);
            throw new IgniteInternalException(e);
        }
    }
}
