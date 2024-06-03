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

package org.apache.ignite.internal.table;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteTransaction;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.test.ItTransactionTestUtils.waitAndGetPrimaryReplica;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.datareplication.network.command.UpdateCommand;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for the transactions running while the primary changes, not related to the tx recovery.
 */
public class ItTransactionPrimaryChangeTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "{\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  rest.port: {},\n"
            + "  raft: { responseTimeout: 30000 },"
            + "  compute.threadPoolSize: 1\n"
            + "}";

    @BeforeEach
    @Override
    public void setup(TestInfo testInfo) throws Exception {
        super.setup(testInfo);

        String zoneSql = "create zone test_zone with partitions=1, replicas=3, storage_profiles='" + DEFAULT_AIPERSIST_PROFILE_NAME + "'";
        String sql = "create table " + TABLE_NAME + " (key int primary key, val varchar(20)) with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(zoneSql, session);
            executeUpdate(sql, session);
        });
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);

        builder.clusterConfiguration("{"
                + "  transaction: {"
                + "      implicitTransactionTimeout: 30000,"
                + "      txnResourceTtl: 2"
                + "  },"
                + "  replication: {"
                + "      rpcTimeout: 30000"
                + "  },"
                + "}");
    }

    /**
     * Returns node bootstrap config template.
     *
     * @return Node bootstrap config template.
     */
    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    public void testFullTxConsistency() throws InterruptedException {
        TableImpl tbl = unwrapTableImpl(node(0).tables().table(TABLE_NAME));

        int partId = 0;

        var tblReplicationGrp = new TablePartitionId(tbl.tableId(), partId);

        String leaseholder = waitAndGetPrimaryReplica(node(0), tblReplicationGrp).getLeaseholder();

        IgniteImpl firstLeaseholderNode = findNodeByName(leaseholder);

        log.info("Test: Full transaction will be executed on [node={}].", firstLeaseholderNode.name());

        IgniteImpl txCrdNode = findNode(0, initialNodes(), n -> !leaseholder.equals(n.name()));

        log.info("Test: Transaction coordinator is [node={}].", txCrdNode.name());

        RecordView<Tuple> view = txCrdNode.tables().table(TABLE_NAME).recordView();

        // Put some value into the table.
        Transaction txPreload = txCrdNode.transactions().begin();
        log.info("Test: Preloading the data [tx={}].", ((ReadWriteTransactionImpl) unwrapIgniteTransaction(txPreload)).id());
        view.upsert(txPreload, Tuple.create().set("key", 1).set("val", "1"));
        txPreload.commit();

        var fullTxReplicationAttemptFuture = new CompletableFuture<>();
        var regularTxComplete = new CompletableFuture<>();

        firstLeaseholderNode.dropMessages((node, msg) -> {
            if (msg instanceof WriteActionRequest) {
                WriteActionRequest writeActionRequest = (WriteActionRequest) msg;

                if (tblReplicationGrp.toString().equals(writeActionRequest.groupId())
                        && writeActionRequest.deserializedCommand() instanceof UpdateCommand
                        && !fullTxReplicationAttemptFuture.isDone()) {
                    UpdateCommand updateCommand = (UpdateCommand) writeActionRequest.deserializedCommand();

                    if (updateCommand.full()) {
                        fullTxReplicationAttemptFuture.complete(null);

                        log.info("Test: Stopped the full tx before sending write command [txId={}].", updateCommand.txId());

                        regularTxComplete.join();
                    }
                }
            }

            return false;
        });

        // Starting the full transaction.
        log.info("Test: Starting the full transaction.");
        CompletableFuture<Tuple> fullTxFut = view.getAndDeleteAsync(null, Tuple.create().set("key", 1));

        try {
            assertThat(fullTxReplicationAttemptFuture, willCompleteSuccessfully());

            // Changing the primary.
            NodeUtils.transferPrimary(cluster.runningNodes().collect(toList()), tblReplicationGrp, txCrdNode.name());

            // Start a regular transaction that increments the value. It should see the initially inserted value and its commit should
            // succeed.
            Transaction tx = txCrdNode.transactions().begin();
            log.info("Test: Started the regular transaction [txId={}].", ((ReadWriteTransactionImpl) unwrapIgniteTransaction(tx)).id());

            Tuple t = view.get(tx, Tuple.create().set("key", 1));
            assertEquals("1", t.value(1));
            view.upsert(tx, Tuple.create().set("key", 1).set("val", "2"));

            tx.commit();

            log.info("Test: Completed the regular transaction [txId={}].", ((ReadWriteTransactionImpl) unwrapIgniteTransaction(tx)).id());
        } finally {
            regularTxComplete.complete(null);
        }

        fullTxFut.join();

        // Full transaction should finally complete.
        assertThat(fullTxFut, willCompleteSuccessfully());

        log.info("Test: Completed the full transaction [txId={}].");

        // Transaction "tx" got the value, this means the full transaction hasn't executed yet at the moment of "tx" commit.
        // Hence the retrieved value should always be 2.
        assertEquals("2", fullTxFut.join().value("val"));
    }

    private IgniteImpl findNode(int startRange, int endRange, Predicate<IgniteImpl> filter) {
        return IntStream.range(startRange, endRange)
                .mapToObj(this::node)
                .filter(filter::test)
                .findFirst()
                .get();
    }

    private IgniteImpl findNodeByName(String leaseholder) {
        return findNode(0, initialNodes(), n -> leaseholder.equals(n.name()));
    }
}
