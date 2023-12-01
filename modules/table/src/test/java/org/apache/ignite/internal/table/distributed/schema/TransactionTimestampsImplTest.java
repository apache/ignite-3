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

package org.apache.ignite.internal.table.distributed.schema;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.AdditionalMatchers.gt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransactionTimestampsImplTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID = 1;

    @Mock
    private CatalogService catalogService;

    private final HybridClock clock = new HybridClockImpl();

    private TransactionTimestamps timestamps;

    private final HybridTimestamp beginTimestamp = clock.now();

    private final HybridTimestamp readTimestamp = clock.now();

    @Mock
    private CatalogTableDescriptor tableDescriptorWithTableVersion1;

    @Mock
    private InternalTransaction transaction;

    @BeforeEach
    void init() {
        timestamps = new TransactionTimestampsImpl(new AlwaysSyncedSchemaSyncService(), catalogService, clock);

        lenient().doReturn(CatalogTableDescriptor.INITIAL_TABLE_VERSION)
                .when(tableDescriptorWithTableVersion1).tableVersion();
    }

    @Test
    void rwTransactionBaseTimestampIsBeginTimestampWhenTableExistsAtIt() {
        when(catalogService.table(TABLE_ID, beginTimestamp.longValue()))
                .thenReturn(mock(CatalogTableDescriptor.class));

        assertThat(timestamps.rwTransactionBaseTimestamp(beginTimestamp, TABLE_ID), willBe(beginTimestamp));
    }

    @Test
    void rwTransactionBaseTimestampIsTableCreationTimestampIfTableCreatedAfterTxStarted() {
        HybridTimestamp tableCreationTimestamp = clock.now();

        when(catalogService.table(TABLE_ID, beginTimestamp.longValue())).thenReturn(null);
        when(catalogService.activeCatalogVersion(beginTimestamp.longValue())).thenReturn(1);
        when(catalogService.activeCatalogVersion(gt(tableCreationTimestamp.longValue()))).thenReturn(3);
        when(catalogService.table(TABLE_ID, 2)).thenReturn(null);
        when(catalogService.table(TABLE_ID, 3)).thenReturn(tableDescriptorWithTableVersion1);
        when(catalogService.catalog(3)).thenReturn(new Catalog(3, tableCreationTimestamp.longValue(), 0, List.of(), List.of()));

        assertThat(timestamps.rwTransactionBaseTimestamp(beginTimestamp, TABLE_ID), willBe(tableCreationTimestamp));
    }

    @Test
    void rwTransactionBaseTimestampIsBeginTimestampIfNoTableAtAll() {
        when(catalogService.table(TABLE_ID, beginTimestamp.longValue())).thenReturn(null);
        when(catalogService.activeCatalogVersion(beginTimestamp.longValue())).thenReturn(1);
        when(catalogService.activeCatalogVersion(gt(beginTimestamp.longValue()))).thenReturn(3);
        when(catalogService.table(TABLE_ID, 2)).thenReturn(null);
        when(catalogService.table(TABLE_ID, 3)).thenReturn(null);

        assertThat(timestamps.rwTransactionBaseTimestamp(beginTimestamp, TABLE_ID), willBe(beginTimestamp));
    }

    @Test
    void baseTimestampIsBeginTimestampWhenRwAndTableExistsAtIt() {
        configureRwTransaction();
        when(catalogService.table(TABLE_ID, beginTimestamp.longValue()))
                .thenReturn(mock(CatalogTableDescriptor.class));

        assertThat(timestamps.baseTimestamp(transaction, TABLE_ID), willBe(beginTimestamp));
    }

    private void configureRwTransaction() {
        when(transaction.isReadOnly()).thenReturn(false);
        when(transaction.startTimestamp()).thenReturn(beginTimestamp);
    }

    @Test
    void baseTimestampIsTableCreationTimestampIfRwAndTableCreatedAfterTxStarted() {
        configureRwTransaction();
        HybridTimestamp tableCreationTimestamp = clock.now();

        when(catalogService.table(TABLE_ID, beginTimestamp.longValue())).thenReturn(null);
        when(catalogService.activeCatalogVersion(beginTimestamp.longValue())).thenReturn(1);
        when(catalogService.activeCatalogVersion(gt(tableCreationTimestamp.longValue()))).thenReturn(3);
        when(catalogService.table(TABLE_ID, 2)).thenReturn(null);
        when(catalogService.table(TABLE_ID, 3)).thenReturn(tableDescriptorWithTableVersion1);
        when(catalogService.catalog(3)).thenReturn(new Catalog(3, tableCreationTimestamp.longValue(), 0, List.of(), List.of()));

        assertThat(timestamps.baseTimestamp(transaction, TABLE_ID), willBe(tableCreationTimestamp));
    }

    @Test
    void baseTimestampIsBeginTimestampIfRwAndNoTableAtAll() {
        configureRwTransaction();

        when(catalogService.table(TABLE_ID, beginTimestamp.longValue())).thenReturn(null);
        when(catalogService.activeCatalogVersion(beginTimestamp.longValue())).thenReturn(1);
        when(catalogService.activeCatalogVersion(gt(beginTimestamp.longValue()))).thenReturn(3);
        when(catalogService.table(TABLE_ID, 2)).thenReturn(null);
        when(catalogService.table(TABLE_ID, 3)).thenReturn(null);

        assertThat(timestamps.baseTimestamp(transaction, TABLE_ID), willBe(beginTimestamp));
    }
    @Test
    void baseTimestampIsReadTimestampWhenRo() {
        configureRoTransaction();

        assertThat(timestamps.baseTimestamp(transaction, TABLE_ID), willBe(readTimestamp));
    }

    private void configureRoTransaction() {
        when(transaction.isReadOnly()).thenReturn(true);
        lenient().when(transaction.readTimestamp()).thenReturn(readTimestamp);
        when(transaction.startTimestamp()).thenReturn(readTimestamp);
    }
}
