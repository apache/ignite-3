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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileChange;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.storage.pagememory.pending.PendingRowsKey;
import org.apache.ignite.internal.storage.pagememory.pending.PendingRowsTree;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link org.apache.ignite.internal.storage.pagememory.pending.PendingRowsTree}.
 */
@ExtendWith(ConfigurationExtension.class)
public class PendingRowsTreeTest extends BaseIgniteAbstractTest {

    @InjectConfiguration(polymorphicExtensions = {VolatilePageMemoryProfileConfigurationSchema.class}, value = "mock.engine = aimem")
    private StorageProfileConfiguration dataRegionCfg;
    private PageMemory pageMem;
    private int GROUP_ID = 1;

    @BeforeEach
    protected void beforeEach() throws Exception {
        pageMem = createPageMemory();

        pageMem.start();
    }

    @AfterEach
    protected void afterTest() {
        if (pageMem != null) {
            pageMem.stop(true);
        }
    }

    protected PageMemory createPageMemory() throws Exception {
        dataRegionCfg
                .change(c -> ((VolatilePageMemoryProfileChange) c)
                        .changeInitSizeBytes(1024 * MiB)
                        .changeMaxSizeBytes(1024 * MiB))
                .get(1, TimeUnit.SECONDS);

        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemory(
                (VolatilePageMemoryProfileConfiguration) fixConfiguration(dataRegionCfg),
                ioRegistry,
                512,
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL)
        );
    }

    private FullPageId allocateMetaPage() throws IgniteInternalCheckedException {
        return new FullPageId(pageMem.allocatePageNoReuse(GROUP_ID, 0, FLAG_AUX), GROUP_ID);
    }


    @Test
    public void test() throws Exception {
        System.out.println("This is a placeholder test for PendingRowsTree.");

        FullPageId metaPage = allocateMetaPage();

        PendingRowsTree tree = new PendingRowsTree(
                metaPage.groupId(),
                "testGroup",
                partitionId(metaPage.pageId()),
                pageMem,
                new AtomicLong(),
                metaPage.pageId(),
                null,
                true
        );

        // Example values for timestamp and RowId
        UUID tx1 = UUID.randomUUID(), tx2 = UUID.randomUUID(), tx3 = UUID.randomUUID();
        RowId rowId1 = new RowId(0, UUID.randomUUID());
        RowId rowId2 = new RowId(0, UUID.randomUUID());
        RowId rowId3 = new RowId(0, UUID.randomUUID());

        PendingRowsKey key1 = new PendingRowsKey(tx1, rowId1);
        PendingRowsKey key2 = new PendingRowsKey(tx2, rowId2);
        PendingRowsKey key3 = new PendingRowsKey(tx3, rowId3);

        // Insert keys
        tree.put(key1);
        tree.put(key2);
        tree.put(key3);

        // Check presence
        assertEquals(key1, tree.findOne(key1));
        assertEquals(key2, tree.findOne(key2));
        assertEquals(key3, tree.findOne(key3));

        // Remove a key and check absence
        tree.remove(key2);
        assertNull(tree.findOne(key2));

        // Check others still present
        assertEquals(key1, tree.findOne(key1));
        assertEquals(key3, tree.findOne(key3));
    }

    @Test
    public void testOrder() throws Exception {
        System.out.println("This is a placeholder test for PendingRowsTree.");

        FullPageId metaPage = allocateMetaPage();

        PendingRowsTree tree = new PendingRowsTree(
                metaPage.groupId(),
                "testGroup",
                partitionId(metaPage.pageId()),
                pageMem,
                new AtomicLong(),
                metaPage.pageId(),
                null,
                true
        );

        // Example values for timestamp and RowId
        UUID tx1 = UUID.randomUUID(), tx2 = UUID.randomUUID(), tx3 = UUID.randomUUID();

        putKeyFotTx(tx1, tree, 10);
        putKeyFotTx(tx2, tree, 20);
        putKeyFotTx(tx3, tree, 30);

        long elements;

        elements = tree.find(new PendingRowsKey(tx1, RowId.lowestRowId(0)), new PendingRowsKey(tx1, RowId.highestRowId(0))).stream()
                .count();

        assertEquals(10, elements);

        elements = tree.find(new PendingRowsKey(tx2, RowId.lowestRowId(0)), new PendingRowsKey(tx2, RowId.highestRowId(0))).stream()
                .count();

        assertEquals(20, elements);

        elements = tree.find(new PendingRowsKey(tx3, RowId.lowestRowId(0)), new PendingRowsKey(tx3, RowId.highestRowId(0))).stream()
                .count();

        assertEquals(30, elements);
    }

    private static void putKeyFotTx(UUID tx1, PendingRowsTree tree, int cnt) throws IgniteInternalCheckedException {
        for (int i = 0; i < cnt; i++) {
            RowId rowId = new RowId(i, UUID.randomUUID());
            PendingRowsKey key = new PendingRowsKey(tx1, rowId);
            tree.put(key);
        }
    }
}
