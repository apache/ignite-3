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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.persistence.PageReadWriteManager;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteLogger;
import org.jetbrains.annotations.Nullable;

/**
 * Waiting IGNITE-17011.
 */
// TODO: IGNITE-17011 ждем
public class FilePageStoreManager implements PageReadWriteManager, IgniteComponent {
    private final Path dbPath;

    private final FileIoFactory fileIoFactory;

    private final int pageSize;

    private final Map<Integer, IgniteBiTuple<FilePageStore, FilePageStore[]>> groupPageStores = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public FilePageStoreManager(
            IgniteLogger log,
            String igniteInstanceName,
            Path storagePath,
            FileIoFactory filePageStoreFileIoFactory,
            // TODO: IGNITE-17017 Move to common config
            int pageSize
    ) throws IgniteInternalCheckedException {
        fileIoFactory = filePageStoreFileIoFactory;
        this.pageSize = pageSize;

        try {
            Files.createDirectories(dbPath = storagePath.resolve("db"));
        } catch (IOException e) {
            throw new IgniteInternalCheckedException(e);
        }
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() throws Exception {
        for (Integer grpId : groupPageStores.keySet()) {
            for (FilePageStore store : getStores(grpId)) {
                store.stop(false);
            }
        }

        groupPageStores.clear();
    }

    /** {@inheritDoc} */
    @Override
    public void read(int grpId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        getStore(grpId, PageIdUtils.partitionId(pageId)).read(pageId, pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override
    public PageStore write(
            int grpId,
            long pageId,
            ByteBuffer pageBuf,
            int tag,
            boolean calculateCrc
    ) throws IgniteInternalCheckedException {
        FilePageStore pageStore = getStore(grpId, PageIdUtils.partitionId(pageId));

        pageStore.write(pageId, pageBuf, tag, calculateCrc);

        return pageStore;
    }

    /** {@inheritDoc} */
    @Override
    public long allocatePage(int grpId, int partId, byte flags) throws IgniteInternalCheckedException {
        long pageIdx = getStore(grpId, partId).allocatePage();

        return PageIdUtils.pageId(partId, flags, (int) pageIdx);
    }

    /**
     * Prepare dirs fot group.
     */
    public void initialize(String grpName, int grpId, int partitions) throws IgniteInternalCheckedException {
        try {
            Path groupDir = Files.createDirectories(dbPath.resolve("group-" + grpName));

            FilePageStore idxFilePageStore = new FilePageStore(PageStore.TYPE_IDX, groupDir.resolve("index.bin"), fileIoFactory, pageSize);

            FilePageStore[] partitionFilePageStores = new FilePageStore[partitions];

            for (int i = 0; i < partitions; i++) {
                partitionFilePageStores[i] = new FilePageStore(
                        PageStore.TYPE_DATA,
                        groupDir.resolve("part-" + i + ".bin"),
                        fileIoFactory,
                        pageSize
                );
            }

            groupPageStores.put(grpId, new IgniteBiTuple<>(idxFilePageStore, partitionFilePageStores));
        } catch (IOException e) {
            throw new IgniteInternalCheckedException(e);
        }
    }

    /**
     * Returns stores.
     */
    public @Nullable Collection<FilePageStore> getStores(int grpId) {
        IgniteBiTuple<FilePageStore, FilePageStore[]> stores = groupPageStores.get(grpId);

        if (stores == null) {
            return null;
        } else {
            List<FilePageStore> res = new ArrayList<>(1 + stores.getValue().length);

            for (FilePageStore filePageStore : stores.getValue()) {
                res.add(filePageStore);
            }

            res.add(stores.getKey());

            return res;
        }
    }

    private FilePageStore getStore(int grpId, int partId) throws IgniteInternalCheckedException {
        IgniteBiTuple<FilePageStore, FilePageStore[]> objects = groupPageStores.get(grpId);

        if (objects == null) {
            throw new IgniteInternalCheckedException("grpId=" + grpId);
        }

        if (partId == PageIdAllocator.INDEX_PARTITION) {
            return objects.getKey();
        } else {
            return objects.getValue()[partId];
        }
    }
}
