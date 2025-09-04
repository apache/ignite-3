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

package org.apache.ignite.internal.sql.engine.statistic;

import java.util.function.LongSupplier;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounter;
import org.apache.ignite.internal.table.distributed.PartitionModificationCounterFactory;

/**
 * Implementation of factory for producing {@link PartitionModificationCounter}.
 */
public class PartitionModificationCounterFactoryImpl implements PartitionModificationCounterFactory {
    public static final long DEFAULT_MIN_STALE_ROWS_COUNT = 500L;

    public static final double DEFAULT_STALE_ROWS_FRACTION = 0.2d;

    private final ClockService clockService;

    public PartitionModificationCounterFactoryImpl(ClockService clockService) {
        this.clockService = clockService;
    }

    @Override
    public PartitionModificationCounter create(LongSupplier partitionSizeSupplier) {
        return new PartitionModificationCounterImpl(
                clockService.now(),
                partitionSizeSupplier,
                DEFAULT_STALE_ROWS_FRACTION,
                DEFAULT_MIN_STALE_ROWS_COUNT
        );
    }
}
