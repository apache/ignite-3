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

package org.apache.ignite.internal.client;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.client.IgniteClient;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(OldClientTestInstanceFactory.class)
@TestInstance(Lifecycle.PER_CLASS)
public class OldClientWithCurrentServerCompatibilityTest implements ClientCompatibilityTests {
    private ClientCompatibilityTests delegate;

    void setDelegate(ClientCompatibilityTests delegate) {
        this.delegate = delegate;
    }

    @Override
    public IgniteClient client() {
        return delegate.client();
    }

    @Override
    public AtomicInteger idGen() {
        return delegate.idGen();
    }

    @Test
    @Override
    public void testClusterNodes() {
        delegate.testClusterNodes();
    }

    @Test
    @Override
    public void testTableByName() {
        delegate.testTableByName();
    }

    @Test
    @Override
    public void testTableByQualifiedName() {
        delegate.testTableByQualifiedName();
    }

    @Test
    @Override
    public void testTables() {
        delegate.testTables();
    }

    @Test
    @Override
    public void testSqlColumnMeta() {
        delegate.testSqlColumnMeta();
    }

    @Test
    @Override
    public void testSqlSelectAllColumnTypes() {
        delegate.testSqlSelectAllColumnTypes();
    }

    @Test
    @Override
    public void testSqlMultiplePages() {
        delegate.testSqlMultiplePages();
    }

    @Test
    @Override
    public void testSqlScript() {
        delegate.testSqlScript();
    }

    @Test
    @Override
    public void testSqlBatch() {
        delegate.testSqlBatch();
    }

    @Test
    @Override
    public void testRecordViewOperations() {
        delegate.testRecordViewOperations();
    }

    @Test
    @Override
    public void testKvViewOperations() {
        delegate.testKvViewOperations();
    }

    @Test
    @Override
    public void testRecordViewAllColumnTypes() {
        delegate.testRecordViewAllColumnTypes();
    }

    @Test
    @Override
    public void testTxCommit() {
        delegate.testTxCommit();
    }

    @Test
    @Override
    public void testTxRollback() {
        delegate.testTxRollback();
    }

    @Test
    @Override
    public void testTxReadOnly() {
        delegate.testTxReadOnly();
    }

    @Test
    @Override
    public void testComputeMissingJob() {
        delegate.testComputeMissingJob();
    }

    @Test
    @Override
    public void testStreamer() {
        delegate.testStreamer();
    }

    @Test
    @Override
    public void testStreamerWithReceiver() {
        if (delegate != null) {
            delegate.testStreamerWithReceiver();
        } else {
            ClientCompatibilityTests.super.testStreamerWithReceiver();
        }
    }

    private static class Delegate implements ClientCompatibilityTests {
        private final AtomicInteger idGen = new AtomicInteger(1000);

        private final IgniteClient client;

        public Delegate(IgniteClient.Builder client) {
            this.client = client.addresses("localhost:10800").build();
        }

        @Override
        public IgniteClient client() {
            return client;
        }

        @Override
        public AtomicInteger idGen() {
            return idGen;
        }
    }
}
