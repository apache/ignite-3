/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.calcite;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.List;
import com.google.common.collect.Lists;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.SqlCursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.NodeStoppingException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stop Calcite module integration test.
 */
public class ITStopCalciteModuleTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected void initTestData() {
        createAndPopulateTable();
    }

    /** */
    @Test
    public void testStopQueryOnNodeStop() throws Exception {
        final QueryProcessor engine = ((IgniteImpl)CLUSTER_NODES.get(0)).queryEngine();

        List<SqlCursor<List<?>>> cursors = engine.query(
            "PUBLIC",
            "SELECT * FROM PERSON"
        );

        SqlCursor<List<?>> cur = cursors.get(0);
        cur.next();

        IgniteUtils.closeAll(Lists.reverse(CLUSTER_NODES));

        // Check cursor closed.
        assertTrue(assertThrows(IgniteException.class, cur::hasNext).getMessage().contains("Query was cancelled"));
        assertTrue(assertThrows(IgniteException.class, cur::next).getMessage().contains("Query was cancelled"));

        // Check execute query on stopped node.
        assertTrue(assertThrows(IgniteException.class, () -> engine.query(
            "PUBLIC",
            "SELECT 1"
        )).getCause() instanceof NodeStoppingException);

        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = bean.dumpAllThreads(true, true);

        // Check: there are no alive Ignite threads.
        assertFalse(Arrays.stream(infos)
            .anyMatch((ti) -> {
                for (Ignite ign : CLUSTER_NODES) {
                    if (ti.getThreadName().contains(ign.name()))
                        return true;
                }

                return false;
            })
        );
    }
}
