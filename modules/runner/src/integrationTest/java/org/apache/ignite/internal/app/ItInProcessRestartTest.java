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

package org.apache.ignite.internal.app;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class ItInProcessRestartTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void restarts() {
        IgniteServerImpl server = (IgniteServerImpl) cluster.server(0);

        assertThat(server.restartAsync(), willCompleteSuccessfully());
    }

    /**
     * Makes sure that operations happening during a restart finish successfully.
     */
    @Test
    void restartIsTransparent() {
        Ignite ignite = node(0);

        ignite.sql().executeScript("CREATE TABLE test (id INT PRIMARY KEY, val VARCHAR)");
        KeyValueView<Integer, String> kvView = ignite.tables().table("test").keyValueView(Integer.class, String.class);

        var insertedSomething = new CompletableFuture<Void>();

        AtomicBoolean restarted = new AtomicBoolean(false);
        AtomicInteger lastInsertedId = new AtomicInteger();

        CompletableFuture<Void> putsFuture = runAsync(() -> {
            for (int i = 0; !restarted.get(); i++) {
                kvView.put(null, i, "value-" + i);

                lastInsertedId.set(i);
                insertedSomething.complete(null);
            }
        });

        IgniteServerImpl server = (IgniteServerImpl) cluster.server(0);
        CompletableFuture<Void> restartedFuture = insertedSomething.thenCompose(unused -> server.restartAsync())
                .whenComplete((res, ex) -> restarted.set(true));
        assertThat(restartedFuture, willCompleteSuccessfully());

        assertThat(putsFuture, willCompleteSuccessfully());

        for (int i = 0; i < lastInsertedId.get(); i++) {
            assertThat(kvView.get(null, i), is("value-" + i));
        }
    }
}
