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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.pagememory.persistence.TestPartitionMeta.FACTORY;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link PartitionMetaManager} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class PartitionMetaManagerTest extends BaseIgniteAbstractTest {
    private static final int PAGE_SIZE = 1024;

    private static PageIoRegistry ioRegistry;

    @BeforeAll
    static void beforeAll() {
        ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();
    }

    @AfterAll
    static void afterAll() {
        ioRegistry = null;
    }

    @Test
    void testAddGetMeta() {
        PartitionMetaManager manager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, FACTORY);

        GroupPartitionId id = new GroupPartitionId(0, 0);

        assertNull(manager.getMeta(id));

        TestPartitionMeta meta = mock(TestPartitionMeta.class);

        manager.addMeta(id, meta);

        assertSame(meta, manager.getMeta(id));

        assertNull(manager.getMeta(new GroupPartitionId(0, 1)));
    }

    @Test
    void testRemoveMeta() {
        PartitionMetaManager manager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, FACTORY);

        GroupPartitionId id = new GroupPartitionId(0, 0);

        manager.removeMeta(id);

        TestPartitionMeta meta = mock(TestPartitionMeta.class);

        manager.addMeta(id, meta);

        manager.removeMeta(id);

        assertNull(manager.getMeta(id));
    }
}
