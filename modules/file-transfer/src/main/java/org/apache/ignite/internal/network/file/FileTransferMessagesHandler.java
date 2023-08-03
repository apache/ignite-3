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
import org.apache.ignite.internal.network.file.messages.FileChunk;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;

class FileTransferMessagesHandler {
    private final Path dir;
    private final AtomicInteger filesCount = new AtomicInteger(-1);
    private final AtomicInteger filesFinished = new AtomicInteger(0);
    private final CompletableFuture<List<File>> result = new CompletableFuture<>();
    private final Map<String, ChunkedFileWriter> fileNameToWriter = new ConcurrentHashMap<>();
    private final Map<String, Lock> fileNameToLock = new ConcurrentHashMap<>();

    FileTransferMessagesHandler(Path dir) {
        this.dir = dir;
    }

    void handleFileTransferInfo(FileTransferInfo info) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file transfer info after result is already done.");
        }

        if (filesCount.get() != -1) {
            throw new IllegalStateException("Received file transfer info twice.");
        } else if (info.filesCount() == 0) {
            result.complete(List.of());
        } else {
            filesCount.set(info.filesCount());
        }
    }

    void handleFileHeader(FileHeader header) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file header after result is already done.");
        }
        doInLock(header.fileName(), () -> handleFileHeader0(header));
    }

    private void handleFileHeader0(FileHeader header) {
        try {
            Path path = Files.createFile(dir.resolve(header.fileName()));
            fileNameToWriter.put(header.fileName(), ChunkedFileWriter.open(path, header.fileSize()));
        } catch (IOException e) {
            handleFileTransferError(e);
        }
    }

    void handleFileChunk(FileChunk fileChunk) {
        if (result.isDone()) {
            throw new IllegalStateException("Received chunked file after result is already done.");
        }
        doInLock(fileChunk.fileName(), () -> handleFileChunk0(fileChunk));
    }

    private void handleFileChunk0(FileChunk fileChunk) {
        try {
            ChunkedFileWriter writer = fileNameToWriter.get(fileChunk.fileName());
            writer.write(fileChunk);

            if (writer.isFinished()) {
                writer.close();
                fileNameToWriter.remove(fileChunk.fileName());
                filesFinished.incrementAndGet();
            }

            if (filesFinished.get() == filesCount.get()) {
                try (Stream<Path> stream = Files.list(dir)) {
                    List<File> files = stream.map(Path::toFile).collect(Collectors.toList());
                    result.complete(files);
                }
            }

        } catch (IOException e) {
            handleFileTransferError(e);
        }
    }

    void handleFileTransferError(Throwable error) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file transfer error after result is already done.");
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
}
