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

import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.compute.utils.Clients;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;

/**
 * Integration tests for Compute functionality using thin client API.
 */
@SuppressWarnings("NewClassNamingConvention")
public class ItComputeTestClient extends ItComputeTestEmbedded {
    private final Clients clients = new Clients();

    @AfterEach
    void stopClient() {
        clients.cleanup();
    }

    @Override
    protected IgniteCompute compute() {
        return clients.compute(node(0));
    }

    @Override
    void cancelsNotCancellableJob(boolean local) {
        // No-op. Embedded-specific.
    }

    @Override
    protected @Nullable HybridTimestamp currentObservableTimestamp() {
        TcpIgniteClient client = (TcpIgniteClient) clients.client(node(0));
        return client.channel().observableTimestamp().get();
    }
}
