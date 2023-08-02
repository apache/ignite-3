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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.network.file.messages.FileChunk;

class ChunkedFileWriter implements ManuallyCloseable {
    private final RandomAccessFile raf;

    private final long fileSize;

    private final Queue<FileChunk> chunks = new PriorityQueue<>(FileChunk.COMPARATOR);

    private ChunkedFileWriter(RandomAccessFile raf, long fileSize) {
        this.raf = raf;
        this.fileSize = fileSize;
    }

    static ChunkedFileWriter open(Path path, long fileSize) throws FileNotFoundException {
        return new ChunkedFileWriter(new RandomAccessFile(path.toFile(), "rw"), fileSize);
    }

    void write(FileChunk chunk) throws IOException {
        chunks.add(chunk);

        while (!chunks.isEmpty() && chunks.peek().offset() == raf.getFilePointer()) {
            raf.write(chunks.poll().data());
        }

        if (raf.getFilePointer() > fileSize) {
            throw new IOException("File size exceeded");
        }
    }

    boolean isFinished() throws IOException {
        return raf.getFilePointer() == fileSize;
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
