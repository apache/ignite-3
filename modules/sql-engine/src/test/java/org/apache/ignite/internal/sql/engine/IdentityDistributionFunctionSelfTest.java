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

package org.apache.ignite.internal.sql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution.Type;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.exec.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.trait.DistributionFunction.IdentityDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Identity distribution function self test.
 */
public class IdentityDistributionFunctionSelfTest {

    private static final String NODE_1 = "node1";
    private static final String NODE_2 = "node2";
    private static final String NODE_3 = "node3";

    private final RowHandler<Object[]> rowHandler = ArrayRowHandler.INSTANCE;

    private final RowSchema rowSchema = RowSchema.builder()
            .addField(NativeTypes.STRING)
            .addField(NativeTypes.STRING)
            .build();

    private final ColocationGroup colocationGroup = ColocationGroup.forNodes(List.of(NODE_1, NODE_2, NODE_3));

    @Test
    public void distributionFunction() {
        IdentityDistribution function = new IdentityDistribution();

        assertThat(function.type(), equalTo(Type.HASH_DISTRIBUTED));
        assertThat(function.affinity(), is(false));
        assertThat(function.name(), equalTo("identity"));

        Destination<Object[]> destination = function.destination(rowHandler, colocationGroup, ImmutableIntList.of(0));

        assertThat(destination.targets().size(), equalTo(3));
        assertThat(destination.targets(), containsInAnyOrder(NODE_1, NODE_2, NODE_3));
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20246")
    @Test
    public void distributionTrait() {
        Object[] row = rowHandler.factory(rowSchema).create(NODE_1, "Ne prikhodya v soznanie");
        Object[] otherRow = rowHandler.factory(rowSchema).create("UNKNOWN", NODE_2);

        IgniteDistribution distribution = IgniteDistributions.identity(0);
        Destination<Object> destination = distribution.destination(null, colocationGroup);

        // Valid row.
        assertThat(destination.targets(row).size(), equalTo(1));
        assertThat(destination.targets(row), Matchers.contains(NODE_1));

        // Invalid row.
        assertThat(destination.targets(otherRow), empty());

        // Apply mapping to function to satisfy otherRow.
        distribution = distribution.apply(Mappings.target(Map.of(0, 1, 1, 0), 2, 2));
        destination = distribution.destination(null, colocationGroup);

        assertThat(destination.targets(otherRow).size(), equalTo(1));
        assertThat(destination.targets(otherRow), Matchers.contains(NODE_2));

    }

    @Test
    public void destination() {
        Object[] row = rowHandler.factory(rowSchema).create(NODE_1, "Ne prikhodya v soznanie");
        Object[] otherRow = rowHandler.factory(rowSchema).create("UNKNOWN", NODE_2);

        IdentityDistribution function = new IdentityDistribution();
        Destination<Object[]> destination = function.destination(rowHandler, colocationGroup, ImmutableIntList.of(0));

        // Valid row.
        assertThat(destination.targets(row).size(), equalTo(1));
        assertThat(destination.targets(row), Matchers.contains(NODE_1));

        // Invalid row.
        Assertions.assertThrows(IllegalStateException.class, () -> destination.targets(otherRow));

        // Apply mapping to function to satisfy otherRow.
        Destination<Object[]> remappedDestination = function.destination(rowHandler, colocationGroup, ImmutableIntList.of(1));

        assertThat(remappedDestination.targets(otherRow).size(), equalTo(1));
        assertThat(remappedDestination.targets(otherRow), Matchers.contains(NODE_2));
    }
}
