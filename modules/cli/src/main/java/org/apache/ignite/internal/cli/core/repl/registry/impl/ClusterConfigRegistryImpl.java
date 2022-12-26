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

package org.apache.ignite.internal.cli.core.repl.registry.impl;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.inject.Singleton;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.internal.cli.core.repl.AsyncSessionEventListener;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.registry.ClusterConfigRegistry;

/** Implementation of {@link ClusterConfigRegistry}. */
@Singleton
public class ClusterConfigRegistryImpl implements ClusterConfigRegistry, AsyncSessionEventListener {

    private final ClusterConfigShowCall clusterConfigShowCall;

    private final AtomicReference<Config> config = new AtomicReference<>(null);

    public ClusterConfigRegistryImpl(ClusterConfigShowCall clusterConfigShowCall) {
        this.clusterConfigShowCall = clusterConfigShowCall;
    }

    @Override
    public void onConnect(Session session) {
        CompletableFuture.runAsync(() -> {
            try {
                config.set(ConfigFactory.parseString(
                        clusterConfigShowCall.execute(
                                ClusterConfigShowCallInput.builder().clusterUrl(session.context().nodeUrl()).build()
                        ).body().getValue()
                ));
            } catch (Exception ignored) {
                // no-op
            }
        });
    }

    @Override
    public void onDisconnect() {

    }

    /** {@inheritDoc} */
    @Override
    public Config config() {
        return config.get();
    }
}
