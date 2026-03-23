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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction.IdentityDistribution;
import org.apache.ignite.internal.sql.engine.trait.Identity;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Identity distribution function self test.
 */
public class IdentityDistributionFunctionSelfTest {

    private static final String NODE_1 = "node1";
    private static final String NODE_2 = "node2";
    private static final String NODE_3 = "node3";

    private final RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;
    private final RowFactoryFactory<Object[]> rowFactoryFactory = ArrayRowHandler.INSTANCE;

    private final StructNativeType rowSchema = NativeTypes.structBuilder()
            .addField("C1", NativeTypes.STRING, true)
            .addField("C2", NativeTypes.STRING, true)
            .build();

    private final ColocationGroup colocationGroup = new ColocationGroup(LongList.of(1L), List.of(NODE_1, NODE_2, NODE_3),
            Int2ObjectMaps.emptyMap());
    private final DestinationFactory<Object[]> destinationFactory = new DestinationFactory<>(rowHandler, null);

    @Test
    public void identityDistributionTrait() {
        IdentityDistribution function = new IdentityDistribution();

        assertThat(function.type(), equalTo(Type.HASH_DISTRIBUTED));
        assertThat(function.name(), equalTo("identity"));
    }

    @Test
    public void destinationTargets() {
        Destination<Object[]> destination = destinationFactory.createDestination(IgniteDistributions.identity(0), colocationGroup);

        assertThat(destination, instanceOf(Identity.class));

        Collection<String> targets = destination.targets();

        assertThat(targets.size(), equalTo(3));
        assertThat(targets, containsInAnyOrder(NODE_1, NODE_2, NODE_3));
    }

    @Test
    public void destinationRowTargets() {
        Object[] row = rowFactoryFactory.create(rowSchema).create(NODE_1, "Ne prikhodya v soznanie");

        Destination<Object[]> destination0 = destinationFactory.createDestination(IgniteDistributions.identity(0), colocationGroup);
        Destination<Object[]> destination1 = destinationFactory.createDestination(IgniteDistributions.identity(1), colocationGroup);

        assertThat(destination0, instanceOf(Identity.class));

        // Valid row has single target.
        Collection<String> targets = destination0.targets(row);

        assertThat(targets.size(), equalTo(1));
        assertThat(targets, Matchers.contains(NODE_1));

        // Validate column mapping
        Object[] otherRow = rowFactoryFactory.create(rowSchema).create("UNKNOWN", NODE_2);
        targets = destination1.targets(otherRow);

        assertThat(targets.size(), equalTo(1));
        assertThat(targets, Matchers.contains(NODE_2));

        // Check wrong mapping.
        Assertions.assertThrows(IllegalStateException.class, () -> destination0.targets(otherRow));
    }

    @Test
    public void destinationForInvalidRow() {
        Destination<Object[]> destination = destinationFactory.createDestination(IgniteDistributions.identity(0), colocationGroup);

        Object[] invalidRow1 = rowFactoryFactory.create(rowSchema).create("UNKNOWN", NODE_2);
        Assertions.assertThrows(IllegalStateException.class, () -> destination.targets(invalidRow1));

        Object[] invalidRow2 = rowFactoryFactory.create(rowSchema).create("", NODE_2);
        Assertions.assertThrows(IllegalStateException.class, () -> destination.targets(invalidRow2));

        Object[] invalidRow3 = rowFactoryFactory.create(rowSchema).create(null, NODE_2);
        Assertions.assertThrows(IllegalStateException.class, () -> destination.targets(invalidRow3));
    }
}
