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

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.ClientRunner;
import org.apache.ignite.internal.CompatibilityTestBase;
import org.apache.ignite.internal.IgniteCluster;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(OldClientTestInstanceFactory.class)
public class OldClientWithCurrentServerCompatibilityTest implements ClientCompatibilityTests {
    private final AtomicInteger idGen = new AtomicInteger(1000);

    private IgniteClient client;

    @WorkDirectory
    private Path workDir;

    public void initClient(IgniteClient.Builder builder) {
        client = builder.addresses("127.0.0.1:10800").build();
    }

    @Test
    public void test(TestInfo testInfo) {
        // TODO: Resource management.
        IgniteCluster cluster = CompatibilityTestBase.createCluster(testInfo, workDir);
        cluster.startEmbedded(1, true);
        createDefaultTables(cluster.createClient());

        ClientRunner.runClient("3.0.0");
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
