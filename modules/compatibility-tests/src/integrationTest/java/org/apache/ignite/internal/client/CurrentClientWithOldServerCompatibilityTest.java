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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.CompatibilityTestBase;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests that current Java client can work with all older server versions.
 */
@ParameterizedClass(name = "{displayName}({argumentsWithNames})")
@MethodSource("serverVersions")
public class CurrentClientWithOldServerCompatibilityTest extends CompatibilityTestBase implements ClientCompatibilityTests {
    private final AtomicInteger idGen = new AtomicInteger(1000);

    private @Nullable IgniteClient client;

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        initTestData(baseIgnite);
    }

    @Override
    protected int nodesCount() {
        return 1;
    }

    @Override
    protected boolean restartWithCurrentEmbeddedVersion() {
        // Keep old servers running.
        return false;
    }

    @BeforeEach
    public void createClient() {
        client = cluster.createClient();
    }

    @AfterEach
    public void closeClient() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    @Override
    public IgniteClient client() {
        return Objects.requireNonNull(client);
    }

    @Override
    public AtomicInteger idGen() {
        return idGen;
    }

    private static List<String> serverVersions() {
        return baseVersions();
    }

    @Override
    public String tableNamePrefix() {
        return "PUBLIC.";
    }
}
