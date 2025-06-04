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

import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Integration tests for binary {@link KeyValueView} API using explicit transaction.
 */
public class ItKeyValueBinaryViewApiExplicitTxnTest extends ItKeyValueBinaryViewApiTest {
    @Override
    TestCaseFactory getFactory(String name) {
        return new TestCaseFactory(name) {
            @Override
            <K, V> BaseTestCase<K, V> create(boolean async, boolean thin, Class<K> keyClass, Class<V> valueClass) {
                assert keyClass == Tuple.class : keyClass;
                assert valueClass == Tuple.class : valueClass;

                KeyValueView<Tuple, Tuple> view = thin
                        ? client.tables().table(tableName).keyValueView()
                        : CLUSTER.aliveNode().tables().table(tableName).keyValueView();

                if (async) {
                    view = new AsyncApiKeyValueViewAdapter<>(view);
                }

                return (BaseTestCase<K, V>) new TxTestCase(async, thin, view, createdTables.get(name), thin ? client : CLUSTER.aliveNode());
            }
        };
    }

    private static class TxTestCase extends TestCase {
        @Override
        protected Executable wrap(Consumer<Transaction> run) {
            return () -> {
                Transaction tx = ignite.transactions().begin();
                run.accept(tx);
                tx.commit();
            };
        }

        TxTestCase(boolean async, boolean thin, KeyValueView<Tuple, Tuple> view, TestTableDefinition tableDefinition, Ignite ignite) {
            super(async, thin, view, tableDefinition, ignite);
        }
    }

    @ParameterizedTest
    @MethodSource("compoundPkTestCases")
    @Override
    public void schemaMismatch(TestCase testCase) {
        super.schemaMismatch(testCase);
    }
}
