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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for class {@link PartitionModificationCounter}.
 */
public class PartitionModificationCounterTest extends BaseIgniteAbstractTest {
    private final PartitionModificationCounterFactory factory =
            new PartitionModificationCounterFactory(() -> HybridTimestamp.hybridTimestamp(1L), mock(MessagingService.class));

    @Test
    void initialValues() {
        // Empty table.
        {
            PartitionModificationCounter counter = factory.create(
                    () -> 0L, () -> new TableStatsStalenessConfiguration(0.5, 200), 0, 0
            );

            assertThat(counter.value(), is(0L));
            assertThat(counter.nextMilestone(), is(200L));
            assertThat(counter.lastMilestoneTimestamp().longValue(), is(1L));
        }

        // Table with 10k rows.
        {
            PartitionModificationCounter counter = factory.create(
                    () -> 10_000L, () -> new TableStatsStalenessConfiguration(0.2, 200), 0, 0
            );

            assertThat(counter.value(), is(0L));
            assertThat(counter.nextMilestone(), is(2000L));
            assertThat(counter.lastMilestoneTimestamp().longValue(), is(1L));

            // A zero update should not change the counter values.
            counter.updateValue(0, HybridTimestamp.MAX_VALUE);

            assertThat(counter.value(), is(0L));
            assertThat(counter.nextMilestone(), is(2000L));
            assertThat(counter.lastMilestoneTimestamp().longValue(), is(1L));
        }
    }

    @Test
    void lastMilestoneTimestampUpdate() {
        int rowsCount = 10_000;
        int threshold = (int) (rowsCount * CatalogUtils.DEFAULT_STALE_ROWS_FRACTION);
        PartitionModificationCounter counter = factory.create(
                () -> rowsCount,
                () -> new TableStatsStalenessConfiguration(
                        CatalogUtils.DEFAULT_STALE_ROWS_FRACTION, CatalogUtils.DEFAULT_MIN_STALE_ROWS_COUNT
                ), 0, 0
        );

        assertThat(counter.lastMilestoneTimestamp().longValue(), is(1L));

        {
            HybridTimestamp commitTime = HybridTimestamp.hybridTimestamp(100L);

            counter.updateValue(threshold, commitTime);

            assertThat(counter.value(), is(2_000L));
            assertThat(counter.nextMilestone(), is(4_000L));
            assertThat(counter.lastMilestoneTimestamp().longValue(), is(commitTime.longValue()));
        }

        {
            HybridTimestamp commitTime = HybridTimestamp.hybridTimestamp(200L);

            counter.updateValue(threshold, commitTime);
            assertThat(counter.value(), is(4_000L));
            assertThat(counter.nextMilestone(), is(6_000L));
            assertThat(counter.lastMilestoneTimestamp().longValue(), is(commitTime.longValue()));
        }
    }

    @Test
    @SuppressWarnings({"ThrowableNotThrown", "ResultOfObjectAllocationIgnored", "DataFlowIssue"})
    void invalidUpdateValues() {
        PartitionModificationCounter counter = factory.create(
                () -> 0L, () -> new TableStatsStalenessConfiguration(0.2, 500), 0, 0
        );

        IgniteTestUtils.assertThrows(NullPointerException.class,
                () -> counter.updateValue(1, null), "commitTimestamp");

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> counter.updateValue(-1, HybridTimestamp.MIN_VALUE),
                "Delta must be non-negative"
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> new PartitionModificationCounter(null, () -> 0L, () -> new TableStatsStalenessConfiguration(
                        0.2, 500
                )),
                "initTimestamp"
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> new PartitionModificationCounter(HybridTimestamp.MIN_VALUE, null, () -> new TableStatsStalenessConfiguration(
                        0.2, 500
                )),
                "partitionSizeSupplier"
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> new PartitionModificationCounter(HybridTimestamp.MIN_VALUE, () -> 0L, null),
                "configurationProvider"
        );
    }
}
