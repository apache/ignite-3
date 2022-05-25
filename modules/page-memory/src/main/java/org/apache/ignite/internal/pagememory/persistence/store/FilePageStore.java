/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.pagememory.persistence.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Waiting IGNITE-17014.
 */
// TODO: IGNITE-17014 - надо дождаться
public class FilePageStore implements PageStore {
    /** Data type, can be {@link PageStore#TYPE_IDX} or {@link PageStore#TYPE_DATA}. */
    private final byte type;

    /** File page store path. */
    private final Path filePath;

    public FilePageStore(byte type, Path filePath) {
        this.type = type;
        this.filePath = filePath;
    }

    @Override
    public void addWriteListener(PageWriteListener listener) {
    }

    @Override
    public void removeWriteListener(PageWriteListener listener) {
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public long allocatePage() throws IgniteInternalCheckedException {
        return 0;
    }

    @Override
    public long pages() {
        return 0;
    }

    @Override
    public boolean read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        return false;
    }

    @Override
    public void readHeader(ByteBuffer buf) {
    }

    @Override
    public void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteInternalCheckedException {
    }

    @Override
    public void sync() {
    }

    @Override
    public void ensure() throws IgniteInternalCheckedException {
        try {
            Files.createFile(filePath);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException(e);
        }
    }

    @Override
    public int version() {
        return 0;
    }

    @Override
    public void stop(boolean clean) throws IgniteInternalCheckedException {
        if (clean) {
            try {
                Files.delete(filePath);
            } catch (IOException e) {
                throw new IgniteInternalCheckedException(e);
            }
        }
    }

    @Override
    public void truncate(int tag) {
    }

    @Override
    public int pageSize() {
        return 0;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void close() {
    }

    /**
     * Returns data type, can be {@link PageStore#TYPE_IDX} or {@link PageStore#TYPE_DATA}.
     */
    byte type() {
        return type;
    }

    /**
     * Returns file page store path.
     */
    Path filePath() {
        return filePath;
    }
}
