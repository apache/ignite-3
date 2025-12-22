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
import static org.apache.ignite.internal.pagememory.util.PartitionlessLinks.writePartitionless;
import static org.apache.ignite.internal.storage.pagememory.mv.WiLinkableRowVersion.NEXT_WRITE_INTENT_LINK_OFFSET;
import static org.apache.ignite.internal.storage.pagememory.mv.WiLinkableRowVersion.PREV_WRITE_INTENT_LINK_OFFSET;
import static org.apache.ignite.internal.util.GridUnsafe.pageSize;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeList;
import org.apache.ignite.internal.pagememory.io.DataPageIo;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.util.PageHandler;
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

        freeList = storage.renewableState.freeList();
    }

    @Override
    protected RowVersion insertFirstRowVersion() {
        WiLinkableRowVersion newVersion = insertWiLinkableRowVersion(NULL_LINK);

        long wiListHeadLink = persistentStorage.lockWriteIntentListHead();
        long newWiListHeadLink = wiListHeadLink;

        try {
            updateWiListLinks(newVersion.link(), new WriteIntentLinks(NULL_LINK, wiListHeadLink));

            newWiListHeadLink = newVersion.link();
        } finally {
            persistentStorage.updateWriteIntentListHeadAndUnlock(newWiListHeadLink);
        }

        return newVersion;
    }

    @Override
    protected RowVersion insertAnotherRowVersion(VersionChain chain, @Nullable RowVersion existingWriteIntent) {
        boolean replacingExistingWriteIntent = chain.isUncommitted();
        assert replacingExistingWriteIntent == (existingWriteIntent != null);

        WiLinkableRowVersion newVersion = insertWiLinkableRowVersion(chain.newestCommittedLink());

        long wiListHeadLink = persistentStorage.lockWriteIntentListHead();
        long newWiListHeadLink = wiListHeadLink;

        try {
            WriteIntentLinks newWiLinks = newWiLinksForReplacement(existingWriteIntent, wiListHeadLink);

            updateWiListLinks(newVersion.link(), newWiLinks);

            // We do not zero out links in the replaced write intent as no one will be able to use them to traverse WI list
            // (as we hold the WI list lock).

            if (newWiLinks.prevWriteIntentLink() == NULL_LINK) {
                // Add our new version to the head of the WI list, because one of the following is true:
                // 1. we are not replacing an existing write intent
                // 2. we are replacing an existing write intent that is not linkable (so it's not included in the WI list)
                // 3. we are replacing an existing write intent that is linkable, but it's pointed to by the WI list head.
                newWiListHeadLink = newVersion.link();
            }
        } finally {
            persistentStorage.updateWriteIntentListHeadAndUnlock(newWiListHeadLink);
        }

        return newVersion;
    }

    private WriteIntentLinks newWiLinksForReplacement(@Nullable RowVersion existingWriteIntent, long wiListHeadLink) {
        assert persistentStorage.writeIntentHeadIsLockedByCurrentThread();

        if (existingWriteIntent instanceof WiLinkableRowVersion) {
            // Re-read the links under WI list lock to be sure they are up-to-date (this is a form of the double-checked locking idiom).
            return persistentStorage.readWriteIntentLinks(existingWriteIntent.link());
        }

        return new WriteIntentLinks(NULL_LINK, wiListHeadLink);
    }

    private WiLinkableRowVersion insertWiLinkableRowVersion(long nextLink) {
        var rowVersion = new WiLinkableRowVersion(rowId, storage.partitionId, nextLink, NULL_LINK, NULL_LINK, row);

        storage.insertRowVersion(rowVersion);

        return rowVersion;
    }

    private void updateWiListLinks(long newRowVersionLink, WriteIntentLinks newWiLinks) {
        assert persistentStorage.writeIntentHeadIsLockedByCurrentThread();

        try {
            freeList.updateDataRow(newRowVersionLink, UpdateWiLinksHandler.INSTANCE, newWiLinks);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error while updating WI links: [link={}, {}]", e, newRowVersionLink, addWriteInfo());
        }

        if (newWiLinks.prevWriteIntentLink() != NULL_LINK) {
            try {
                // TODO: https://issues.apache.org/jira/browse/IGNITE-27235 - move updateDataRow() from FreeList.
                freeList.updateDataRow(newWiLinks.prevWriteIntentLink(), UpdateNextWiLinkHandler.INSTANCE, newRowVersionLink);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Error while updating WI next link of prev WI: [link={}, {}]",
                        e,
                        newWiLinks.prevWriteIntentLink(),
                        addWriteInfo()
                );
            }
        }

        if (newWiLinks.nextWriteIntentLink() != NULL_LINK) {
            try {
                freeList.updateDataRow(newWiLinks.nextWriteIntentLink(), UpdatePrevWiLinkHandler.INSTANCE, newRowVersionLink);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException(
                        "Error while updating WI prev link of next WI: [link={}, {}]",
                        e,
                        newWiLinks.nextWriteIntentLink(),
                        addWriteInfo()
                );
            }
        }
    }

    private static class UpdateWiLinksHandler implements PageHandler<WriteIntentLinks, Object> {
        private static final UpdateWiLinksHandler INSTANCE = new UpdateWiLinksHandler();

        @Override
        public Object run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo io,
                WriteIntentLinks wiLinks,
                int itemId
        ) {
            DataPageIo dataIo = (DataPageIo) io;

            int payloadOffset = dataIo.getPayloadOffset(pageAddr, itemId, pageSize(), 0);

            writePartitionless(pageAddr + payloadOffset + PREV_WRITE_INTENT_LINK_OFFSET, wiLinks.prevWriteIntentLink());
            writePartitionless(pageAddr + payloadOffset + NEXT_WRITE_INTENT_LINK_OFFSET, wiLinks.nextWriteIntentLink());

            return true;
        }
    }
}
