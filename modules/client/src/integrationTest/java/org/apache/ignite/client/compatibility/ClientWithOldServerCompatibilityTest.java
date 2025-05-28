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

package org.apache.ignite.client.compatibility;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.compatibility.containers.IgniteServerContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that current Java client can work with all older server versions.
 */
public class ClientWithOldServerCompatibilityTest {
    private static IgniteServerContainer serverContainer;
    private IgniteClient client;

    @BeforeAll
    static void beforeAll() {
        serverContainer = new IgniteServerContainer("3.0.0");
        serverContainer.start();

        // TODO: Init the cluster with REST API.
        System.out.println("\n>>>Container started with client port: " + serverContainer.clientPort());
    }

    @AfterAll
    static void afterAll() {
        if (serverContainer != null) {
            serverContainer.stop();
        }
    }

    @BeforeEach
    void beforeEach() {
        client = IgniteClient.builder()
                .addresses("localhost:" + serverContainer.clientPort())
                .build();
    }

    @AfterEach
    void afterEach() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void test() {
        client.clusterNodes();
    }
}
