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
import java.io.RandomAccessFile;
import org.apache.ignite.internal.close.ManuallyCloseable;

class ChunkedFileReader implements ManuallyCloseable {
    private final String fileName;
    private final RandomAccessFile raf;
    private final int chunkSize;

    private ChunkedFileReader(String fileName, RandomAccessFile raf, int chunkSize) {
        this.fileName = fileName;
        this.raf = raf;
        this.chunkSize = chunkSize;
    }

    static ChunkedFileReader open(File file, int chunkSize) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        return new ChunkedFileReader(file.getName(), raf, chunkSize);
    }

    byte[] readNextChunk() throws IOException {
        int toRead = (int) Math.min(chunkSize, raf.length() - raf.getFilePointer());
        byte[] data = new byte[toRead];
        raf.read(data);
        return data;
    }

    public String fileName() {
        return fileName;
    }

    long offset() throws IOException {
        return raf.getFilePointer();
    }

    long length() throws IOException {
        return raf.length();
    }

    boolean isFinished() throws IOException {
        return raf.getFilePointer() == raf.length();
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
