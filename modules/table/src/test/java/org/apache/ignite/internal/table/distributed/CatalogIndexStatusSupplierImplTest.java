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

package org.apache.ignite.internal.table.distributed;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.AVAILABLE;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.REGISTERED;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus.STOPPING;
import static org.apache.ignite.internal.table.TableTestUtils.INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.PK_INDEX_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.TABLE_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleHashIndex;
import static org.apache.ignite.internal.table.TableTestUtils.createSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.dropIndex;
import static org.apache.ignite.internal.table.TableTestUtils.dropSimpleTable;
import static org.apache.ignite.internal.table.TableTestUtils.getIndexIdStrict;
import static org.apache.ignite.internal.table.TableTestUtils.makeIndexAvailable;
import static org.apache.ignite.internal.table.TableTestUtils.startBuildingIndex;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.stopAll;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogTestUtils;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.table.TableTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For {@link CatalogIndexStatusSupplierImpl} testing. */
public class CatalogIndexStatusSupplierImplTest {
    private final TestLowWatermark lowWatermark = new TestLowWatermark();

    private final HybridClock clock = new HybridClockImpl();

    private CatalogManager catalogManager;

    private CatalogIndexStatusSupplierImpl indexStatusSupplier;

    @BeforeEach
    void setUp() {
        catalogManager = CatalogTestUtils.createTestCatalogManager("test", clock);

        indexStatusSupplier = new CatalogIndexStatusSupplierImpl(catalogManager, lowWatermark);

        assertThat(allOf(catalogManager.start(), indexStatusSupplier.start()), willCompleteSuccessfully());

        createSimpleTable(catalogManager, TABLE_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        stopAll(indexStatusSupplier, catalogManager);
    }

    @ParameterizedTest(name = "recovery={0}")
    @ValueSource(booleans = {false, true})
    void testNotExistsIndex(boolean recovery) throws Exception {
        if (recovery) {
            recreateIndexStatusSupplier();
        }

        assertNull(indexStatusSupplier.get(Integer.MAX_VALUE));
    }

    @ParameterizedTest(name = "recovery={0}")
    @ValueSource(booleans = {false, true})
    void testGetStatusSimple(boolean recovery) throws Exception {
        Map<CatalogIndexStatus, Integer> indexIdByStatus = createIndexes(CatalogIndexStatus.values());

        if (recovery) {
            recreateIndexStatusSupplier();
        }

        assertEquals(AVAILABLE, indexStatusSupplier.get(indexId(PK_INDEX_NAME)));

        for (Entry<CatalogIndexStatus, Integer> e : indexIdByStatus.entrySet()) {
            CatalogIndexStatus indexStatus = e.getKey();

            assertEquals(indexStatus, indexStatusSupplier.get(e.getValue()), indexStatus.toString());
        }
    }

    @ParameterizedTest(name = "recovery={0}")
    @ValueSource(booleans = {false, true})
    void testGetStatusForRemovedIndex(boolean recovery) throws Exception {
        Map<CatalogIndexStatus, Integer> indexIdByStatus = createIndexes(CatalogIndexStatus.values());

        indexIdByStatus.forEach(this::removeIndex);

        int pkIndexId = indexId(PK_INDEX_NAME);

        dropSimpleTable(catalogManager, TABLE_NAME);

        if (recovery) {
            recreateIndexStatusSupplier();
        }

        assertEquals(AVAILABLE, indexStatusSupplier.get(pkIndexId));

        for (Entry<CatalogIndexStatus, Integer> e : indexIdByStatus.entrySet()) {
            CatalogIndexStatus indexStatus = e.getKey();

            if (indexStatus == AVAILABLE) {
                assertEquals(STOPPING, indexStatusSupplier.get(e.getValue()));
            } else {
                assertEquals(indexStatus, indexStatusSupplier.get(e.getValue()), indexStatus.toString());
            }
        }
    }

    @ParameterizedTest(name = "recovery={0}")
    @ValueSource(booleans = {false, true})
    void testGetStatusForRemovedIndexWithUpdateLwm(boolean recovery) throws Exception {
        Map<CatalogIndexStatus, Integer> indexIdByStatus = createIndexes(CatalogIndexStatus.values());

        indexIdByStatus.forEach(this::removeIndex);

        int pkIndexId = indexId(PK_INDEX_NAME);

        dropSimpleTable(catalogManager, TABLE_NAME);

        assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());

        if (recovery) {
            recreateIndexStatusSupplier();
        }

        assertNull(indexStatusSupplier.get(pkIndexId));

        for (Entry<CatalogIndexStatus, Integer> e : indexIdByStatus.entrySet()) {
            assertNull(indexStatusSupplier.get(e.getValue()), e.getKey().toString());
        }
    }

    @Test
    void testRecoveryDropIndexEventQueue() throws Exception {
        int registeredIndexId = createIndex(REGISTERED);
        int availableIndexId = createIndex(AVAILABLE);

        removeIndex(REGISTERED, registeredIndexId);

        assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());

        removeIndex(AVAILABLE, availableIndexId);

        assertNull(indexStatusSupplier.get(registeredIndexId));
        assertEquals(STOPPING, indexStatusSupplier.get(availableIndexId));

        recreateIndexStatusSupplier();

        assertNull(indexStatusSupplier.get(registeredIndexId));
        assertEquals(STOPPING, indexStatusSupplier.get(availableIndexId));

        assertThat(lowWatermark.updateAndNotify(clock.now()), willCompleteSuccessfully());

        assertNull(indexStatusSupplier.get(registeredIndexId));
        assertNull(indexStatusSupplier.get(availableIndexId));
    }

    private void recreateIndexStatusSupplier() throws Exception {
        stopAll(indexStatusSupplier);

        indexStatusSupplier = new CatalogIndexStatusSupplierImpl(catalogManager, lowWatermark);

        assertThat(indexStatusSupplier.start(), willCompleteSuccessfully());
    }

    private int createIndex(CatalogIndexStatus indexStatus) {
        String indexName = indexName(indexStatus);

        switch (indexStatus) {
            case REGISTERED:
                createSimpleHashIndex(catalogManager, TABLE_NAME, indexName);
                return indexId(indexStatus);

            case BUILDING:
                createSimpleHashIndex(catalogManager, TABLE_NAME, indexName);
                startBuildingIndex(catalogManager, indexId(indexStatus));
                return indexId(indexStatus);

            case AVAILABLE:
                createSimpleHashIndex(catalogManager, TABLE_NAME, indexName);
                startBuildingIndex(catalogManager, indexId(indexStatus));
                makeIndexAvailable(catalogManager, indexId(indexStatus));
                return indexId(indexStatus);

            case STOPPING:
                createSimpleHashIndex(catalogManager, TABLE_NAME, indexName);
                startBuildingIndex(catalogManager, indexId(indexStatus));
                makeIndexAvailable(catalogManager, indexId(indexStatus));

                int indexId = indexId(indexStatus);
                dropIndex(catalogManager, DEFAULT_SCHEMA_NAME, indexName);
                return indexId;

            default:
                throw new AssertionError("Unknown status: " + indexStatus);
        }
    }

    private void removeIndex(CatalogIndexStatus indexStatus, int indexId) {
        String indexName = indexName(indexStatus);

        switch (indexStatus) {
            case REGISTERED:
            case BUILDING:
                dropIndex(catalogManager, DEFAULT_SCHEMA_NAME, indexName);
                break;

            case AVAILABLE:
                dropIndex(catalogManager, DEFAULT_SCHEMA_NAME, indexName);
                TableTestUtils.removeIndex(catalogManager, indexId);
                break;

            case STOPPING:
                TableTestUtils.removeIndex(catalogManager, indexId);
                break;

            default:
                throw new AssertionError("Unknown status: " + indexStatus);
        }
    }

    private Map<CatalogIndexStatus, Integer> createIndexes(CatalogIndexStatus... indexStatuses) {
        var res = new HashMap<CatalogIndexStatus, Integer>();

        for (CatalogIndexStatus indexStatus : indexStatuses) {
            res.put(indexStatus, createIndex(indexStatus));
        }

        return res;
    }

    private int indexId(CatalogIndexStatus indexStatus) {
        return indexId(indexName(indexStatus));
    }

    private int indexId(String indexName) {
        return getIndexIdStrict(catalogManager, indexName, clock.nowLong());
    }

    private static String indexName(CatalogIndexStatus indexStatus) {
        return indexStatus + "_" + INDEX_NAME;
    }
}
