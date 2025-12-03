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

import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.storage.StorageException;

class WriteIntentListSupport {
    static void removeNodeFromWriteIntentsList(
            long linkToRowVersionToRemove,
            PersistentPageMemoryMvPartitionStorage storage,
            Supplier<String> operationInfoSupplier
    ) {
        FreeListImpl freeList = storage.renewableState.freeList();

        long wiListHeadLink = storage.lockWriteIntentListHead();

        try {
            WriteIntentLinks links = storage.readWriteIntentLinks(linkToRowVersionToRemove);

            if (links.nextWriteIntentLink() != NULL_LINK) {
                freeList.updateDataRow(
                        links.nextWriteIntentLink(),
                        UpdatePrevWiLinkHandler.INSTANCE,
                        links.prevWriteIntentLink()
                );
            }

            if (links.prevWriteIntentLink() != NULL_LINK) {
                freeList.updateDataRow(
                        links.prevWriteIntentLink(),
                        UpdateNextWiLinkHandler.INSTANCE,
                        links.nextWriteIntentLink()
                );
            } else {
                wiListHeadLink = links.nextWriteIntentLink();
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Error while updating WI links: [link={}, {}]",
                    e,
                    linkToRowVersionToRemove,
                    operationInfoSupplier.get()
            );
        } finally {
            storage.updateWriteIntentListHeadAndUnlock(wiListHeadLink);
        }
    }
}
