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

import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * NOT NULL constraint test for thin client API.
 */
public class ItNotNullConstraintClientTest extends ItNotNullConstraintTest {

    private IgniteClient client;

    @BeforeEach
    public void startClient() {
        client = IgniteClient.builder()
                .addresses("localhost:" + CLUSTER.aliveNode().clientAddress().port())
                .build();
    }

    @AfterEach
    public void stopClient() throws Exception {
        client.close();
    }

    @Override
    protected Ignite ignite() {
        return client;
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-22040 is resolved error messages for client and server API should be the same.
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22040")
    @Override
    public void testKeyValueView() {
        super.testKeyValueView();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22040")
    @Override
    public void testRecordView() {
        super.testRecordView();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22040")
    @Override
    public void testKeyValueViewDataStreamer() {
        super.testRecordView();
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22040")
    @Override
    public void testRecordViewDataStreamer() {
        super.testRecordView();
    }
}
