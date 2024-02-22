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

package org.apache.ignite.internal.pagememory.freelist;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileChange;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolder;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Class to test the {@link AbstractFreeList}.
 */
@ExtendWith(ConfigurationExtension.class)
public class AbstractFreeListTest extends BaseIgniteAbstractTest {
    private static final long MAX_SIZE = 1024 * MiB;

    private static final int BATCH_SIZE = 100;

    @InjectConfiguration(polymorphicExtensions = VolatilePageMemoryProfileConfigurationSchema.class, value = "mock.engine = aimem")
    private StorageProfileConfiguration storageProfileCfg;

    @Nullable
    private PageMemory pageMemory;

    @AfterEach
    void afterEach() {
        if (pageMemory != null) {
            pageMemory.stop(true);
        }
    }

    @ParameterizedTest
    @MethodSource("provideTestArguments")
    void testSingleTread(int pageSize, boolean batched) throws Exception {
        FreeList<TestDataRow> freeList = createFreeList(pageSize);

        Map<Long, TestDataRow> stored = new HashMap<>();

        prepare(freeList, stored);

        insertDeleteRows(freeList, stored, new AtomicBoolean(true), batched);
    }

    @ParameterizedTest
    @MethodSource("provideTestArguments")
    void testMultiThread(int pageSize, boolean batched) throws Exception {
        FreeList<TestDataRow> freeList = createFreeList(pageSize);

        Map<Long, TestDataRow> stored = new ConcurrentHashMap<>();

        AtomicBoolean grow = new AtomicBoolean(true);

        prepare(freeList, stored);

        runMultiThreadedAsync(
                () -> {
                    insertDeleteRows(freeList, stored, grow, batched);

                    return null;
                },
                4,
                "runner"
        ).get(1, MINUTES);
    }

    private static Stream<Arguments> provideTestArguments() {
        return Stream.of(
                Arguments.of(1024, true),
                Arguments.of(1024, false),
                //
                Arguments.of(2048, true),
                Arguments.of(2048, false),
                //
                Arguments.of(4096, true),
                Arguments.of(4096, false),
                //
                Arguments.of(8192, true),
                Arguments.of(8192, false),
                //
                Arguments.of(16384, true),
                Arguments.of(16384, false)
        );
    }

    private FreeList<TestDataRow> createFreeList(int pageSize) throws Exception {
        pageMemory = createPageMemory(pageSize);

        pageMemory.start();

        long metaPageId = pageMemory.allocatePage(1, 1, FLAG_DATA);

        return new AbstractFreeList<>(
                0,
                1,
                "freelist",
                pageMemory,
                null,
                PageLockListenerNoOp.INSTANCE,
                log,
                metaPageId,
                true,
                null,
                PageEvictionTrackerNoOp.INSTANCE
        ) {
            /** {@inheritDoc} */
            @Override
            public void insertDataRow(TestDataRow row, IoStatisticsHolder statHolder) throws IgniteInternalCheckedException {
                super.insertDataRow(row, statHolder);

                assertEquals(row.partition(), partitionId(row.link()));
            }
        };
    }

    private PageMemory createPageMemory(int pageSize) throws Exception {
        storageProfileCfg
                .change(c -> ((VolatilePageMemoryProfileChange) c)
                        .changeInitSize(MAX_SIZE)
                        .changeMaxSize(MAX_SIZE))
                .get(1, TimeUnit.SECONDS);

        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        ioRegistry.load(TestDataPageIo.VERSIONS);

        return new VolatilePageMemory(
                (VolatilePageMemoryProfileConfiguration) fixConfiguration(storageProfileCfg),
                ioRegistry,
                pageSize
        );
    }

    private void prepare(FreeList<TestDataRow> freeList, Map<Long, TestDataRow> stored) throws Exception {
        Random rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 100; i++) {
            int size = rnd.nextInt(pageMemory.pageSize() * 3 / 2) + 10;

            TestDataRow row = new TestDataRow(size);

            freeList.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE);

            assertNotEquals(0L, row.link());

            TestDataRow oldRow = stored.put(row.link(), row);

            assertNull(oldRow);
        }
    }

    private void insertDeleteRows(
            FreeList<TestDataRow> freeList,
            Map<Long, TestDataRow> stored,
            AtomicBoolean grow,
            boolean batched
    ) throws Exception {
        List<TestDataRow> rows = new ArrayList<>(BATCH_SIZE);

        Random rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 200_000; i++) {
            boolean grow0 = grow.get();

            if (grow0) {
                if (stored.size() > 20_000) {
                    if (grow.compareAndSet(true, false)) {
                        log.info("Shrink... [" + stored.size() + ']');
                    }

                    grow0 = false;
                }
            } else {
                if (stored.size() < 1_000) {
                    if (grow.compareAndSet(false, true)) {
                        log.info("Grow... [" + stored.size() + ']');
                    }

                    grow0 = true;
                }
            }

            boolean insert = rnd.nextInt(100) < 70 == grow0;

            if (insert) {
                int size = rnd.nextInt(pageMemory.pageSize() * 3 / 2) + 10;

                TestDataRow row = new TestDataRow(size);

                if (batched) {
                    rows.add(row);

                    if (rows.size() == BATCH_SIZE) {
                        freeList.insertDataRows(rows, IoStatisticsHolderNoOp.INSTANCE);

                        for (TestDataRow row0 : rows) {
                            assertNotEquals(0L, row0.link());

                            TestDataRow old = stored.put(row0.link(), row0);

                            assertNull(old);
                        }

                        rows.clear();
                    }

                    continue;
                }

                freeList.insertDataRow(row, IoStatisticsHolderNoOp.INSTANCE);

                assertNotEquals(0L, row.link());

                TestDataRow old = stored.put(row.link(), row);

                assertNull(old);
            } else {
                while (!stored.isEmpty()) {
                    Iterator<TestDataRow> it = stored.values().iterator();

                    if (it.hasNext()) {
                        TestDataRow row = it.next();

                        TestDataRow rmvd = stored.remove(row.link());

                        if (rmvd != null) {
                            freeList.removeDataRowByLink(row.link(), IoStatisticsHolderNoOp.INSTANCE);

                            break;
                        }
                    }
                }
            }
        }
    }
}
