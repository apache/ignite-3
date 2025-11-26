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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.tx.TxState.ABORTED;
import static org.apache.ignite.internal.tx.TxState.COMMITTED;
import static org.apache.ignite.internal.tx.TxState.PENDING;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class for testing local map of tx states.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItTxStateLocalMapTest extends IgniteAbstractTest {
    private static final int NODES = 3;

    // TODO fsync can be turned on again after https://issues.apache.org/jira/browse/IGNITE-20195
    @InjectConfiguration("mock: { fsync: false }")
    private RaftConfiguration raftConfig;

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemLocalConfiguration;

    @InjectConfiguration("mock.properties.txnLockRetryCount=\"0\"")
    private SystemDistributedConfiguration systemDistributedConfiguration;

    @InjectConfiguration
    private ReplicationConfiguration replicationConfiguration;

    private final TestInfo testInfo;

    private ItTxTestCluster testCluster;

    private TableViewInternal table;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxStateLocalMapTest(TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    protected static final SchemaDescriptor TABLE_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("col1".toUpperCase(), NativeTypes.INT64, false)},
            new Column[]{new Column("col2".toUpperCase(), NativeTypes.INT32, false)}
    );

    @BeforeEach
    public void before() throws Exception {
        testCluster = new ItTxTestCluster(
                testInfo,
                raftConfig,
                txConfiguration,
                systemLocalConfiguration,
                systemDistributedConfiguration,
                workDir,
                NODES,
                NODES,
                false,
                HybridTimestampTracker.atomicTracker(null),
                replicationConfiguration
        );

        testCluster.prepareCluster();

        table = testCluster.startTable("table", TABLE_SCHEMA);
    }

    @AfterEach
    public void after() throws Exception {
        testCluster.shutdownCluster();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testUpsert(boolean commit) {
        testTransaction(tx -> table.recordView().upsert(tx, makeValue(1, 1)), true, commit, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testGet(boolean commit) {
        testTransaction(tx -> table.recordView().get(tx, makeKey(1)), false, commit, true);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    public void testUpdateAll(boolean commit) {
        testTransaction(tx -> table.recordView().upsertAll(tx, List.of(makeValue(1, 1), makeValue(2, 2))), true, commit, false);
    }

    private void testTransaction(Consumer<Transaction> touchOp, boolean checkAfterTouch, boolean commit, boolean read) {
        InternalClusterNode coord = testCluster.cluster.get(0).topologyService().localMember();
        UUID coordinatorId = coord.id();

        ReadWriteTransactionImpl tx = (ReadWriteTransactionImpl) testCluster.igniteTransactions().begin();

        checkLocalTxStateOnNodes(tx.id(), new TxStateMeta(PENDING, coordinatorId, tx.commitPartition(), null, null, null), List.of(0));
        checkLocalTxStateOnNodes(tx.id(), null, IntStream.range(1, NODES).boxed().collect(toList()));

        touchOp.accept(tx);

        if (checkAfterTouch) {
            checkLocalTxStateOnNodes(tx.id(), new TxStateMeta(PENDING, coordinatorId, tx.commitPartition(), null, null, null));
        }

        if (commit) {
            tx.commit();
        } else {
            tx.rollback();
        }

        if (read) {
            checkLocalTxStateOnNodes(tx.id(), null);
        } else {
            checkLocalTxStateOnNodes(
                    tx.id(),
                    new TxStateMeta(
                            commit ? COMMITTED : ABORTED,
                            coordinatorId,
                            tx.commitPartition(),
                            commit ? testCluster.clockServices.get(coord.name()).now() : null,
                            null,
                            null
                    )
            );
        }
    }

    private void checkLocalTxStateOnNodes(UUID txId, TxStateMeta expected) {
        checkLocalTxStateOnNodes(txId, expected, IntStream.range(0, NODES).boxed().collect(toList()));
    }

    private void checkLocalTxStateOnNodes(UUID txId, TxStateMeta expected, List<Integer> nodesToCheck) {
        for (int i : nodesToCheck) {
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

                    return (expected.txState() == meta.get().txState()
                            && checkTimestamps(expected.commitTimestamp(), meta.get().commitTimestamp()));
                }, 5_000));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (AssertionError e) {
                throw new AssertionError("node index=" + i + ", expected=" + expected + ", actual=" + meta.get(), e);
            }
        }
    }

    private boolean checkTimestamps(@Nullable HybridTimestamp expected, @Nullable HybridTimestamp actual) {
        if (expected == null) {
            return actual == null;
        } else {
            if (actual == null) {
                return false;
            }

            return expected.compareTo(actual) > 0;
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
