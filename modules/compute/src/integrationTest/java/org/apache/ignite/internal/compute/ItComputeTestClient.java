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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.IgniteCompute;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

/**
 * Integration tests for Compute functionality using thin client API.
 */
@SuppressWarnings("NewClassNamingConvention")
public class ItComputeTestClient extends ItComputeTestEmbedded {
    private IgniteClient client;

    @BeforeEach
    void startClient() {
        int port = unwrapIgniteImpl(node(0)).clientAddress().port();

        client = IgniteClient.builder()
                .addresses("localhost:" + port)
                .build();
    }

    @AfterEach
    void stopClient() {
        client.close();
    }

    @Override
    protected IgniteCompute compute() {
        return client.compute();
    }

    @Override
    void cancelsNotCancellableJob(boolean local) {
        // No-op. Embedded-specific.
    }

    @Override
    @Disabled("IGNITE-24059 Compute argument serialization is inconsistent in embedded and thin client")
    void executesJobLocally() {
        super.executesJobLocally();
    }

    @Override
    @Disabled("IGNITE-24059 Compute argument serialization is inconsistent in embedded and thin client")
    void executesJobLocallyAsync() {
        super.executesJobLocallyAsync();
    }

    @Override
    @Disabled("IGNITE-24059 Compute argument serialization is inconsistent in embedded and thin client")
    void broadcastsJobWithArgumentsAsync() {
        super.broadcastsJobWithArgumentsAsync();
    }

    @Override
    @Disabled("IGNITE-24059 Compute argument serialization is inconsistent in embedded and thin client")
    void executesJobOnRemoteNodes() {
        super.executesJobOnRemoteNodes();
    }

    @Override
    @Disabled("IGNITE-24059 Compute argument serialization is inconsistent in embedded and thin client")
    void executesJobOnRemoteNodesAsync() {
        super.executesJobOnRemoteNodesAsync();
    }
}
