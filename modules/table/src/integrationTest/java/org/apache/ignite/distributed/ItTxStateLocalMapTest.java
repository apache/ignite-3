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

package org.apache.ignite.distributed;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITED;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Class for testing local map of tx states.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItTxStateLocalMapTest extends IgniteAbstractTest {
    private static final int NODES = 3;

    //TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    private static RaftConfiguration raftConfig;

    @InjectConfiguration
    private static GcConfiguration gcConfig;

    @InjectConfiguration("mock.tables.foo {}")
    private static TablesConfiguration tablesConfig;

    private final TestInfo testInfo;

    private ItTxTestCluster testCluster;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxStateLocalMapTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    protected static SchemaDescriptor TABLE_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("col1".toUpperCase(), NativeTypes.INT64, false)},
            new Column[]{new Column("col2".toUpperCase(), NativeTypes.INT32, false)}
    );

    @BeforeEach
    public void before() throws Exception {
        testCluster = new ItTxTestCluster(
                testInfo,
                raftConfig,
                gcConfig,
                tablesConfig,
                workDir,
                NODES,
                NODES,
                false
        );

        testCluster.prepareCluster();
    }

    @AfterEach
    public void after() throws Exception {
        testCluster.shutdownCluster();
    }

    @Test
    public void testUpsertCommit() throws Exception {
        TableImpl table = testCluster.startTable("table", 1, TABLE_SCHEMA);

        ReadWriteTransactionImpl tx = (ReadWriteTransactionImpl) testCluster.igniteTransactions().begin();

        table.recordView().upsert(tx, makeValue(1, 1));

        ClusterNode coord = testCluster.cluster.get(0).topologyService().localMember();
        String coordinatorId = coord.id();

        checkLocalTxStateOnNodes(tx.id(), new TxStateMeta(PENDING, coordinatorId, null));

        tx.commit();

        checkLocalTxStateOnNodes(tx.id(), new TxStateMeta(COMMITED, coordinatorId, testCluster.clocks.get(coord.name()).now()));
    }

    @Test
    public void testGetAbort() throws Exception {
        TableImpl table = testCluster.startTable("table", 1, TABLE_SCHEMA);

        ReadWriteTransactionImpl tx = (ReadWriteTransactionImpl) testCluster.igniteTransactions().begin();

        table.recordView().get(tx, makeKey(1));

        ClusterNode coord = testCluster.cluster.get(0).topologyService().localMember();
        String coordinatorId = coord.id();

        tx.rollback();

        checkLocalTxStateOnNodes(tx.id(), new TxStateMeta(ABORTED, coordinatorId, null));
    }

    @Test
    public void testUpdateAll() throws Exception {
        TableImpl table = testCluster.startTable("table", 1, TABLE_SCHEMA);

        ReadWriteTransactionImpl tx = (ReadWriteTransactionImpl) testCluster.igniteTransactions().begin();

        List<Tuple> values = List.of(makeValue(1, 1), makeValue(2, 2));

        table.recordView().upsertAll(tx, values);

        ClusterNode coord = testCluster.cluster.get(0).topologyService().localMember();
        String coordinatorId = coord.id();

        checkLocalTxStateOnNodes(tx.id(), new TxStateMeta(PENDING, coordinatorId, null));

        tx.rollback();

        checkLocalTxStateOnNodes(tx.id(), new TxStateMeta(ABORTED, coordinatorId, null));
    }

    private void checkLocalTxStateOnNodes(UUID txId, TxStateMeta expected) {
        for (int i = 0; i < NODES; i++) {
            AtomicReference<TxStateMeta> meta = new AtomicReference<>();

            try {
                int finalI = i;
                assertTrue(waitForCondition(() -> {
                    meta.set(testCluster.txManagers.get(testCluster.cluster.get(finalI).nodeName()).stateMeta(txId));

                    if (expected == null) {
                        return meta.get() == null;
                    } else if (meta.get() == null) {
                        return false;
                    }

                    if (expected.commitTimestamp() == null) {
                        return meta.get().commitTimestamp() == null;
                    } else if (meta.get().commitTimestamp() == null) {
                        return false;
                    }

                    return (expected.txState() == meta.get().txState() && expected.txCoordinatorId().equals(meta.get().txCoordinatorId())
                        && (expected.commitTimestamp() == null && meta.get().commitTimestamp() == null
                            || expected.commitTimestamp().compareTo(meta.get().commitTimestamp()) > 0));
                }, 5_000));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (AssertionError e) {
                throw new AssertionError("node index=" + i + ", expected=" + expected + ", actual=" + meta.get(), e);
            }
        }
    }

    /**
     * Makes a tuple containing key and value.
     *
     * @param id The id.
     * @return The value tuple.
     */
    protected Tuple makeKey(long id) {
        return Tuple.create().set("col1", id);
    }

    /**
     * Makes a tuple containing key and value.
     *
     * @param id The id.
     * @param val The value.
     * @return The value tuple.
     */
    protected Tuple makeValue(long id, int val) {
        return Tuple.create().set("col1", id).set("col2", val);
    }
}
