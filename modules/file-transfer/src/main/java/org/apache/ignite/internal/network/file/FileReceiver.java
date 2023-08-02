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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.network.file.messages.FileChunk;
import org.apache.ignite.internal.network.file.messages.FileHeader;
import org.apache.ignite.internal.network.file.messages.FileTransferInfo;
import org.apache.ignite.internal.util.IgniteUtils;

class FileReceiver implements ManuallyCloseable {
    private final Path dir;
    private final AtomicInteger filesCount = new AtomicInteger(-1);
    private final AtomicInteger filesFinished = new AtomicInteger(0);
    private final CompletableFuture<Path> result = new CompletableFuture<>();
    private final Map<String, ChunkedFileWriter> fileNameToWriter = new ConcurrentHashMap<>();
    private final Map<String, Lock> fileNameToLock = new ConcurrentHashMap<>();

    FileReceiver(Path dir) {
        this.dir = dir;
    }

    void receiveFileTransferInfo(FileTransferInfo info) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file transfer info after result is already done.");
        }
        filesCount.set(info.filesCount());
    }

    void receiveFileHeader(FileHeader header) {
        if (result.isDone()) {
            throw new IllegalStateException("Received file header after result is already done.");
        }
        doInLock(header.fileName(), () -> receiveFileHeader0(header));
    }

    private void receiveFileHeader0(FileHeader header) {
        try {
            Path path = Files.createFile(dir.resolve(header.fileName()));
            fileNameToWriter.put(header.fileName(), ChunkedFileWriter.open(path, header.fileSize()));
        } catch (IOException e) {
            result.completeExceptionally(e);
        }
    }

    void receiveFileChunk(FileChunk fileChunk) {
        if (result.isDone()) {
            throw new IllegalStateException("Received chunked file after result is already done.");
        }
        doInLock(fileChunk.fileName(), () -> receiveFileChunk0(fileChunk));
    }

    private void receiveFileChunk0(FileChunk fileChunk) {
        try {
            ChunkedFileWriter writer = fileNameToWriter.get(fileChunk.fileName());
            writer.write(fileChunk);

            if (writer.isFinished()) {
                writer.close();
                fileNameToWriter.remove(fileChunk.fileName());
                filesFinished.incrementAndGet();
            }

            if (filesFinished.get() == filesCount.get()) {
                result.complete(dir);
            }

        } catch (IOException e) {
            result.completeExceptionally(e);
        }
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

    CompletableFuture<Path> result() {
        return result;
    }

    Path dir() {
        return dir;
    }

    @Override
    public void close() throws Exception {
        IgniteUtils.closeAllManually(fileNameToWriter.values().stream());
    }
}
