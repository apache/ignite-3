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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.ALWAYS_LOAD_VALUE;
import static org.apache.ignite.internal.storage.pagememory.mv.AbstractPageMemoryMvPartitionStorage.rowVersionToResultNotFillingLastCommittedTs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;

/**
 * Cursor reading all versions for {@link RowId}.
 *
 * <p>In the constructor, reads all versions of the chain and then iterates over them.
 */
class ScanVersionsCursor implements Cursor<ReadResult> {
    private final AbstractPageMemoryMvPartitionStorage storage;

    private final VersionChain versionChain;

    private final Iterator<RowVersion> rowVersionIterator;

    /**
     * Constructor.
     *
     * @param versionChain Version chain.
     * @param storage Multi-versioned partition storage.
     * @throws StorageException If there is an error when collecting all versions of the chain.
     */
    ScanVersionsCursor(VersionChain versionChain, AbstractPageMemoryMvPartitionStorage storage) {
        this.storage = storage;
        this.versionChain = versionChain;
        this.rowVersionIterator = collectRowVersions();
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public boolean hasNext() {
        return storage.busy(() -> {
            storage.throwExceptionIfStorageNotInRunnableState();

            return rowVersionIterator.hasNext();
        });
    }

    @Override
    public ReadResult next() {
        return storage.busy(() -> {
            storage.throwExceptionIfStorageNotInRunnableState();

            return rowVersionToResultNotFillingLastCommittedTs(versionChain, rowVersionIterator.next());
        });
    }

    private Iterator<RowVersion> collectRowVersions() {
        long link = versionChain.headLink();

        List<RowVersion> rowVersions = new ArrayList<>();

        while (link != NULL_LINK) {
            RowVersion rowVersion = storage.readRowVersion(link, ALWAYS_LOAD_VALUE);

            if (rowVersion == null) {
                link = NULL_LINK;
            } else {
                rowVersions.add(rowVersion);

                link = rowVersion.nextLink();
            }
        }

        return rowVersions.iterator();
    }
}
