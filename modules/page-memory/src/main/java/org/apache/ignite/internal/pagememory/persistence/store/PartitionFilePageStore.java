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
import java.nio.file.Path;
import org.apache.ignite.internal.pagememory.persistence.PartitionMeta;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * File page store with partition meta.
 */
public class PartitionFilePageStore implements PageStore {
    public final FilePageStore filePageStore;

    private final PartitionMeta partitionMeta;

    /**
     * Constructor.
     *
     * @param filePageStore File page store.
     * @param partitionMeta Partition meta.
     */
    public PartitionFilePageStore(FilePageStore filePageStore, PartitionMeta partitionMeta) {
        this.filePageStore = filePageStore;
        this.partitionMeta = partitionMeta;
    }

    /** {@inheritDoc} */
    @Override
    public void stop(boolean clean) throws IgniteInternalCheckedException {
        filePageStore.stop(clean);
    }

    /** {@inheritDoc} */
    @Override
    public int allocatePage() throws IgniteInternalCheckedException {
        return filePageStore.allocatePage();
    }

    /** {@inheritDoc} */
    @Override
    public int pages() {
        return filePageStore.pages();
    }

    /** {@inheritDoc} */
    @Override
    public boolean read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        return filePageStore.read(pageId, pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override
    public void write(long pageId, ByteBuffer pageBuf, boolean calculateCrc) throws IgniteInternalCheckedException {
        filePageStore.write(pageId, pageBuf, calculateCrc);
    }

    /** {@inheritDoc} */
    @Override
    public void sync() throws IgniteInternalCheckedException {
        filePageStore.sync();
    }

    /** {@inheritDoc} */
    @Override
    public boolean exists() {
        return filePageStore.exists();
    }

    /** {@inheritDoc} */
    @Override
    public void ensure() throws IgniteInternalCheckedException {
        filePageStore.ensure();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        filePageStore.close();
    }

    /**
     * Returns partition meta.
     */
    public PartitionMeta meta() {
        return partitionMeta;
    }

    /**
     * Returns file page store path.
     */
    Path filePath() {
        return filePageStore.filePath();
    }
}
