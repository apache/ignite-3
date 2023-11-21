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

import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataRegion.calculateCheckpointBufferSize;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataRegion.calculateSegmentSizes;
import static org.apache.ignite.internal.util.Constants.GiB;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryDataRegionView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link PersistentPageMemoryDataRegion} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class PersistentPageMemoryDataRegionTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private PersistentPageMemoryDataRegionConfiguration dataRegionConfig;

    @Test
    void testCalculateSegmentSizes() throws Exception {
        int concurrencyLevel = 2;

        PersistentPageMemoryDataRegionView dataRegionConfigView = dataRegionConfig.value();

        assertArrayEquals(
                fill(new long[concurrencyLevel], dataRegionConfigView.size() / concurrencyLevel),
                calculateSegmentSizes(dataRegionConfigView, concurrencyLevel)
        );

        dataRegionConfig.size().update(1024L).get(1, TimeUnit.SECONDS);

        assertArrayEquals(
                fill(new long[concurrencyLevel], MiB),
                calculateSegmentSizes(dataRegionConfig.value(), concurrencyLevel)
        );
    }

    @Test
    void testCalculateCheckpointBufferSize() throws Exception {
        dataRegionConfig.size().update(GiB / 4L).get(1, TimeUnit.SECONDS);

        assertEquals(GiB / 4L, calculateCheckpointBufferSize(dataRegionConfig.value()));

        dataRegionConfig.size().update(GiB / 2L).get(1, TimeUnit.SECONDS);

        assertEquals(GiB / 4L, calculateCheckpointBufferSize(dataRegionConfig.value()));

        dataRegionConfig.size().update(6L * GiB).get(1, TimeUnit.SECONDS);

        assertEquals((6L * GiB) / 4L, calculateCheckpointBufferSize(dataRegionConfig.value()));

        dataRegionConfig.size().update(8L * GiB).get(1, TimeUnit.SECONDS);

        assertEquals(2L * GiB, calculateCheckpointBufferSize(dataRegionConfig.value()));
    }

    private long[] fill(long[] arr, long v) {
        Arrays.fill(arr, v);

        return arr;
    }
}
