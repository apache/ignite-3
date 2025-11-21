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

import static org.apache.ignite.internal.sql.engine.util.QueryChecker.matches;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.tx.TransactionOptions;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests to make sure sql engine can recover execution when run on unstable topology.
 */
public class ItUnstableTopologyTest extends BaseSqlIntegrationTest {
    private static final String DATA_NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} },\n"
            + "  nodeAttributes: {\n"
            + "    nodeAttributes: { role: data }\n"
            + "  },\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    private static final Pattern NODE_NAME_PATTERN = Pattern.compile(".*?=\\[(?<nodeName>.*?)=\\{.*");

    private int additionalNodesCount = 0; 

    @Override
    protected int initialNodes() {
        return 1;
    }

    @AfterEach
    public void dropTables() {
        dropAllTables();
        dropAllZonesExceptDefaultOne();

        while (additionalNodesCount > 0) {
            CLUSTER.stopNode(additionalNodesCount--);
        }
    }

    @ParameterizedTest
    @EnumSource(TxType.class)
    void ensureLostOfNodeDoesntCausesQueryToFail(TxType txType) {
        startAdditionalNode();
        startAdditionalNode();
        startAdditionalNode();

        sql("CREATE ZONE my_zone ("
                + "partitions 1, "
                + "replicas 3, "
                + "auto scale down off, "
                + "nodes filter '$[?(@.role == \"data\")]') "
                + "storage profiles ['default']");

        sql("CREATE TABLE my_table (id INT PRIMARY KEY, val INT) ZONE my_zone STORAGE PROFILE 'default'");
        assertQuery("INSERT INTO my_table SELECT x, x FROM system_range(1, 1000)")
                .returns(1000L)
                .check();

        Ignite gateway = CLUSTER.node(0);

        txType.runInTx(gateway, tx -> assertQuery(gateway, tx, "SELECT count(*) FROM my_table WHERE val > -1")
                .matches(matches(".*TableScan.*"))
                .returns(1000L)
                .check());

        List<List<Object>> mapping = sql(gateway, "EXPLAIN MAPPING FOR SELECT count(*) FROM my_table WHERE val > -1");

        String nodeName = extractAnyNode(mapping);

        assertThat(nodeName, notNullValue());

        CLUSTER.stopNode(nodeName);

        txType.runInTx(gateway, tx -> assertQuery(gateway, tx, "SELECT count(*) FROM my_table WHERE val > -1")
                .matches(matches(".*TableScan.*"))
                .returns(1000L)
                .check());
    }

    private static @Nullable String extractAnyNode(List<List<Object>> explainMappingResult) {
        if (explainMappingResult.size() != 1 || explainMappingResult.get(0).size() != 1) {
            return null;
        }

        String result = explainMappingResult.get(0).get(0).toString();

        Matcher matcher = NODE_NAME_PATTERN.matcher(result.replace("\n", ""));

        if (matcher.matches()) {
            return matcher.group("nodeName");
        }

        return null;
    }

    private void startAdditionalNode() {
        CLUSTER.startNode(++additionalNodesCount, DATA_NODE_BOOTSTRAP_CFG_TEMPLATE);
    }

    enum TxType {
        IMPLICIT {
            @Override
            void runInTx(Ignite node, Consumer<@Nullable InternalTransaction> action) {
                action.accept(null);
            }
        },

        RO {
            @Override
            void runInTx(Ignite node, Consumer<@Nullable InternalTransaction> action) {
                node.transactions().runInTransaction(
                        tx -> {
                            action.accept((InternalTransaction) tx);
                        },
                        new TransactionOptions().readOnly(true)
                );
            }
        },

        RW {
            @Override
            void runInTx(Ignite node, Consumer<@Nullable InternalTransaction> action) {
                node.transactions().runInTransaction(
                        tx -> {
                            action.accept((InternalTransaction) tx);
                        },
                        new TransactionOptions().readOnly(false)
                );
            }
        };

        abstract void runInTx(Ignite node, Consumer<@Nullable InternalTransaction> action); 
    }
}
