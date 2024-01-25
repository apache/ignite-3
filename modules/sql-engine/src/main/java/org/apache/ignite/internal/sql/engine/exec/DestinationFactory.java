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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.PartitionResolverImpl;
import org.apache.ignite.internal.sql.engine.exec.mapping.RowAwareAssignmentResolverImpl;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.AllNodes;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction.AffinityDistribution;
import org.apache.ignite.internal.sql.engine.trait.Identity;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.Partitioned;
import org.apache.ignite.internal.sql.engine.trait.RandomNode;
import org.apache.ignite.internal.sql.engine.util.Commons;

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

                if ("identity".equals(function.name())) {
                    assert !nullOrEmpty(group.nodeNames()) && !nullOrEmpty(keys) && keys.size() == 1;

                    return new Identity<>(rowHandler, keys.get(0), group.nodeNames());
                }

                assert !nullOrEmpty(group.assignments()) && !nullOrEmpty(keys);

                List<String> assignments = Commons.transform(group.assignments(), NodeWithTerm::name);

                if (function.affinity()) {
                    int tableId = ((AffinityDistribution) function).tableId();

                    TableDescriptor tableDescriptor = dependencies.tableDescriptor(tableId);

                    ImmutableIntList tableDistrKeys = tableDescriptor.distribution().getKeys();

                    assert keys.size() == tableDistrKeys.size() : "distribution keys=" + keys + ", "
                            + "table distribution keys=" + tableDistrKeys;

                    var resolver = new PartitionResolverImpl<>(group.assignments().size(), tableDescriptor, rowHandler);

                    return new Partitioned<>(assignments, resolver);
                }

                var resolver = new RowAwareAssignmentResolverImpl<>(group.assignments().size(), keys.toIntArray(), rowHandler);

                return new Partitioned<>(assignments, resolver);
            }
            default:
                throw new IllegalStateException("Unsupported distribution function.");
        }
    }
}
