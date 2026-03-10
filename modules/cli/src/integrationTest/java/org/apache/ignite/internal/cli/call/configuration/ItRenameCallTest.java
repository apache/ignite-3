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

package org.apache.ignite.internal.cli.call.configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.CliIntegrationTest;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCall;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterRenameCallInput;
import org.apache.ignite.internal.cli.call.cluster.status.ClusterStatusCall;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.call.UrlCallInput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ClusterRenameCall}.
 */
public class ItRenameCallTest extends CliIntegrationTest {
    private static final UrlCallInput URL_CALL_INPUT = new UrlCallInput(NODE_URL);

    @Inject
    ClusterRenameCall renameCall;

    @Inject
    ClusterStatusCall statusCall;

    @Test
    @DisplayName("Should rename the cluster")
    public void testRename() {
        String name = readClusterName();
        assertEquals("cluster", name);

        var input = ClusterRenameCallInput.builder()
                .clusterUrl(NODE_URL)
                .name("cluster2")
                .build();

        DefaultCallOutput<String> output = renameCall.execute(input);
        assertFalse(output.hasError());
        assertThat(output.body()).contains("Cluster was renamed successfully");

        name = readClusterName();
        assertEquals("cluster2", name);
    }

    private String readClusterName() {
        return statusCall.execute(URL_CALL_INPUT).body().getName();
    }
}
