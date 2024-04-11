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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.AllNodes;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction.AffinityDistribution;
import org.apache.ignite.internal.sql.engine.trait.Identity;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.Partitioned;
import org.apache.ignite.internal.sql.engine.trait.RandomNode;

/**
 * Factory that resolves {@link IgniteDistribution} trait, which represents logical {@link DistributionFunction} function, into its
 * physical representation - {@link Destination} function.
 */
class DestinationFactory<RowT> {
    private final RowHandler<RowT> rowHandler;
    private final ResolvedDependencies dependencies;

    /**
     * Constructor.
     *
     * @param rowHandler Row handler.
     * @param dependencies Dependencies required to resolve row value dependent distributions.
     */
    DestinationFactory(RowHandler<RowT> rowHandler, ResolvedDependencies dependencies) {
        this.rowHandler = rowHandler;
        this.dependencies = dependencies;
    }

    /**
     * Creates a destination based on given distribution and nodes mapping.
     *
     * @param distribution Distribution function.
     * @param group Target mapping.
     * @return Destination function.
     */
    Destination<RowT> createDestination(IgniteDistribution distribution, ColocationGroup group) {
        DistributionFunction function = distribution.function();

        switch (function.type()) {
            case SINGLETON:
                assert group.nodeNames() != null && group.nodeNames().size() == 1;

                return new AllNodes<>(Collections.singletonList(Objects.requireNonNull(first(group.nodeNames()))));
            case BROADCAST_DISTRIBUTED:
                assert !nullOrEmpty(group.nodeNames());

                return new AllNodes<>(group.nodeNames());
            case RANDOM_DISTRIBUTED:
                assert !nullOrEmpty(group.nodeNames());

                return new RandomNode<>(group.nodeNames());
            case HASH_DISTRIBUTED: {
                ImmutableIntList keys = distribution.getKeys();

                assert !nullOrEmpty(keys);

                if ("identity".equals(function.name())) {
                    assert !nullOrEmpty(group.nodeNames()) && keys.size() == 1;

                    return new Identity<>(rowHandler, keys.get(0), group.nodeNames());
                }

                if (function.affinity()) {
                    assert !nullOrEmpty(group.assignments());

                    int tableId = ((AffinityDistribution) function).tableId();
                    Supplier<PartitionCalculator> calculator = dependencies.partitionCalculator(tableId);
                    TableDescriptor tableDescriptor = dependencies.tableDescriptor(tableId);

                    var resolver = new TablePartitionExtractor<>(calculator.get(), keys.toIntArray(), tableDescriptor, rowHandler);

                    Map<Integer, String> partToNode = group.assignments().int2ObjectEntrySet().stream()
                            .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().name()));

                    return new Partitioned<>(partToNode, resolver);
                }

                var resolver = new RehashingPartitionExtractor<>(group.nodeNames().size(), keys.toIntArray(), rowHandler);

                Int2ObjectMap<String> partToNode = new Int2ObjectOpenHashMap<>();
                int pos = 0;

                for (String name : group.nodeNames()) {
                    partToNode.put(pos++, name);
                }

                return new Partitioned<>(partToNode, resolver);
            }
            default:
                throw new IllegalStateException("Unsupported distribution function.");
        }
    }
}
