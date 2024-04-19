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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.calcite.rel.core.JoinRelType.RIGHT;
import static org.apache.ignite.internal.sql.engine.exec.rel.ExecutionTest.assert2DimArrayEquals;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/** Yash join execution tests. */
public class HashJoinExecutionTest extends AbstractJoinExecutionTest {
    @Override
    JoinAlgo joinAlgo() {
        return JoinAlgo.HASH;
    }

    @Test
    public void testHashJoinRewind() {
        ExecutionContext<Object[]> ctx = executionContext(true);

        ScanNode<Object[]> persons = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{0, "Igor", 1},
                new Object[]{1, "Roman", 2},
                new Object[]{2, "Ivan", 5},
                new Object[]{3, "Alexey", 1}
        ));

        ScanNode<Object[]> deps = new ScanNode<>(ctx, Arrays.asList(
                new Object[]{1, "Core"},
                new Object[]{2, "SQL"},
                new Object[]{3, "QA"}
        ));

        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType outType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));
        RelDataType leftType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf, NativeTypes.INT32, NativeTypes.STRING));
        RelDataType rightType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));

        AbstractRightMaterializedJoinNode<Object[]> join = HashJoinNode.create(ctx, outType, leftType, rightType, RIGHT,
                JoinInfo.of(ImmutableIntList.of(0), ImmutableIntList.of(2)));

        join.register(asList(deps, persons));

        ProjectNode<Object[]> project = new ProjectNode<>(ctx, r -> new Object[]{r[2], r[3], r[1]});
        project.register(join);

        RootRewindable<Object[]> node = new RootRewindable<>(ctx);
        node.register(project);

        assert node.hasNext();

        ArrayList<Object[]> rows = new ArrayList<>();

        while (node.hasNext()) {
            rows.add(node.next());
        }

        assertEquals(4, rows.size());

        Object[][] expected = {
                {0, "Igor", "Core"},
                {3, "Alexey", "Core"},
                {1, "Roman", "SQL"},
                {2, "Ivan", null}
        };

        assert2DimArrayEquals(expected, rows);

        List<Object[]> depsRes = new ArrayList<>();
        depsRes.add(new Object[]{5, "QA"});

        deps = new ScanNode<>(ctx, depsRes);

        join.register(asList(deps, persons));

        node.rewind();

        assert node.hasNext();

        ArrayList<Object[]> rowsAfterRewind = new ArrayList<>();

        while (node.hasNext()) {
            rowsAfterRewind.add(node.next());
        }

        assertEquals(4, rowsAfterRewind.size());

        Object[][] expectedAfterRewind = {
                {2, "Ivan", "QA"},
                {1, "Roman", null},
                {0, "Igor", null},
                {3, "Alexey", null},
        };

        assert2DimArrayEquals(expectedAfterRewind, rowsAfterRewind);
    }
}
