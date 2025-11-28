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

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link InvokeClosure} for {@link AbstractPageMemoryMvPartitionStorage#addWrite(RowId, BinaryRow, UUID, int, int)}
 * which additionally maintains links between write intents.
 *
 * <p>See {@link AbstractPageMemoryMvPartitionStorage} about synchronization.
 *
 * <p>Operation may throw {@link StorageException} which will cause form {@link BplusTree#invoke(Object, Object, InvokeClosure)}.
 */
class AddWriteLinkingWiInvokeClosure extends AddWriteInvokeClosure {
    private final PersistentPageMemoryMvPartitionStorage persistentStorage;

    private final FreeList freeList;

    AddWriteLinkingWiInvokeClosure(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitPartitionId,
            PersistentPageMemoryMvPartitionStorage storage
    ) {
        super(rowId, row, txId, commitZoneId, commitPartitionId, storage);

        persistentStorage = storage;

        this.freeList = storage.renewableState.freeList();
    }

    @Override
    protected RowVersion insertFirstRowVersion() {
        long wiListHeadLink = persistentStorage.lockWriteIntentListHead();
        long newWiListHeadLink = wiListHeadLink;

        try {
            WiLinkableRowVersion newVersion = insertRowVersion(NULL_LINK, wiListHeadLink, NULL_LINK);

            newWiListHeadLink = newVersion.link();

            updateWiListLinks(newVersion);

            return newVersion;
        } finally {
            persistentStorage.updateWriteIntentListHeadAndUnlock(newWiListHeadLink);
        }
    }

    @Override
    protected RowVersion insertAnotherRowVersion(VersionChain oldRow, @Nullable RowVersion existingWriteIntent) {
        boolean replacingExistingWriteIntent = oldRow.isUncommitted();
        assert replacingExistingWriteIntent == (existingWriteIntent != null);

        long wiListHeadLink = persistentStorage.lockWriteIntentListHead();
        long newWiListHeadLink = wiListHeadLink;

        try {
            long newNextWiLink;
            long newPrevWiLink;
            if (replacingExistingWriteIntent) {
                newNextWiLink = existingWriteIntent.operations().nextWriteIntentLink(wiListHeadLink);
                newPrevWiLink = existingWriteIntent.operations().prevWriteIntentLink();
            } else {
                newNextWiLink = wiListHeadLink;
                newPrevWiLink = NULL_LINK;
            }

            WiLinkableRowVersion newVersion = insertRowVersion(oldRow.newestCommittedLink(), newNextWiLink, newPrevWiLink);

            if (!replacingExistingWriteIntent) {
                // Add our new version to the head of the WI list.
                newWiListHeadLink = newVersion.link();
            }

            updateWiListLinks(newVersion);

            return newVersion;
        } finally {
            persistentStorage.updateWriteIntentListHeadAndUnlock(newWiListHeadLink);
        }
    }

    private WiLinkableRowVersion insertRowVersion(long nextLink, long nextWiLink, long prevWiLink) {
        var rowVersion = new WiLinkableRowVersion(rowId, storage.partitionId, nextLink, nextWiLink, prevWiLink, row);

        storage.insertRowVersion(rowVersion);

        return rowVersion;
    }

    private void updateWiListLinks(WiLinkableRowVersion newRowVersion) {
        if (newRowVersion != null) {
            if (newRowVersion.prevWriteIntentLink() != NULL_LINK) {
                try {
                    freeList.updateDataRow(newRowVersion.prevWriteIntentLink(), UpdateNextWiLinkHandler.INSTANCE, newRowVersion.link());
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException(
                            "Error while updating WI next link: [link={}, {}]",
                            e,
                            newRowVersion.prevWriteIntentLink(),
                            addWriteInfo()
                    );
                }
            }

            if (newRowVersion.nextWriteIntentLink() != NULL_LINK) {
                try {
                    freeList.updateDataRow(newRowVersion.nextWriteIntentLink(), UpdatePrevWiLinkHandler.INSTANCE, newRowVersion.link());
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException(
                            "Error while updating WI prev link: [link={}, {}]",
                            e,
                            newRowVersion.nextWriteIntentLink(),
                            addWriteInfo()
                    );
                }
            }
        }
    }
}
