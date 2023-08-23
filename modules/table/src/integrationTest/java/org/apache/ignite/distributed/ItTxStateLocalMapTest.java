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
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
public class ItTxStateLocalMapTest extends IgniteAbstractTest {
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

    protected static SchemaDescriptor ACCOUNTS_SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("accountNumber".toUpperCase(), NativeTypes.INT64, false)},
            new Column[]{new Column("balance".toUpperCase(), NativeTypes.DOUBLE, false)}
    );

    @BeforeEach
    public void before() throws Exception {
        testCluster = new ItTxTestCluster(
                testInfo,
                raftConfig,
                gcConfig,
                tablesConfig,
                workDir,
                3,
                3,
                false
        );

        testCluster.prepareCluster();
    }

    @AfterEach
    public void after() throws Exception {
        testCluster.shutdownCluster();
    }

    @Test
    public void test() throws Exception {
        TableImpl table = testCluster.startTable("accounts", 1, ACCOUNTS_SCHEMA);

        ReadWriteTransactionImpl tx = (ReadWriteTransactionImpl) testCluster.igniteTransactions().begin();

        table.recordView().upsert(tx, makeValue(1, 1));

        testCluster.txManagers.get(testCluster.cluster.get(0).nodeName()).state(tx.id());
    }

    /**
     * Makes a tuple containing key and value.
     *
     * @param id The id.
     * @param balance The balance.
     * @return The value tuple.
     */
    protected Tuple makeValue(long id, double balance) {
        return Tuple.create().set("accountNumber", id).set("balance", balance);
    }
}
