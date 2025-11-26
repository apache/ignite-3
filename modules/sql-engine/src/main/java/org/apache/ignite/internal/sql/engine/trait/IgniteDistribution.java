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

package org.apache.ignite.internal.sql.engine.trait;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;

/**
 * Ignite distribution trait.
 */
public interface IgniteDistribution extends RelDistribution {
    /**
     * Get distribution function.
     */
    DistributionFunction function();

    /** Returns {@code true} if this is table distribution, {@code false} otherwise. */
    boolean isTableDistribution();

    /** Returns zone id of table distribution. */
    int zoneId();

    /** Returns table id of table distribution. */
    int tableId();

    /** Returns distribution function label for EXPLAIN plan purposes.  */
    String label();

    /** {@inheritDoc} */
    @Override
    ImmutableIntList getKeys();

    /** {@inheritDoc} */
    @Override
    IgniteDistribution apply(Mappings.TargetMapping mapping);
}
