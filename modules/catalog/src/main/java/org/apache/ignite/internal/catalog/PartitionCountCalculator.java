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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;

/**
 * Function that calculates how much partitions a zone should use in case partition count parameter wasn't specified by a user.
 */
@FunctionalInterface
public interface PartitionCountCalculator {
    /**
     * Calculates partition count depends on the given parameters set.
     *
     * @param params Container of parameters that may be used to calculate partition count.
     * @return Partition count for a zone.
     */
    int calculate(PartitionCountCalculationParameters params);

    static PartitionCountCalculator fixedPartitionCountCalculator() {
        return params -> DEFAULT_PARTITION_COUNT;
    }
}
