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
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.ClusterConfigShowCallInput;
import org.apache.ignite.internal.cli.call.configuration.JsonString;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.ClusterConfigRegistry;
import org.apache.ignite.internal.cli.event.ConnectionEventListener;
import org.jetbrains.annotations.Nullable;

/** Implementation of {@link ClusterConfigRegistry}. */
@Singleton
public class ClusterConfigRegistryImpl implements ClusterConfigRegistry, ConnectionEventListener {

    private final ClusterConfigShowCall clusterConfigShowCall;

    private LazyObjectRef<Config> configRef;

    public ClusterConfigRegistryImpl(ClusterConfigShowCall clusterConfigShowCall) {
        this.clusterConfigShowCall = clusterConfigShowCall;
    }

    @Override
    public void onConnect(SessionInfo sessionInfo) {
        configRef = new LazyObjectRef<>(() -> fetchConfig(sessionInfo));
    }

    @Nullable
    private Config fetchConfig(SessionInfo sessionInfo) {
        ClusterConfigShowCallInput input = ClusterConfigShowCallInput.builder().clusterUrl(sessionInfo.nodeUrl()).build();
        DefaultCallOutput<JsonString> output = clusterConfigShowCall.execute(input);
        if (output.hasError()) {
            return null;
        }
        return ConfigFactory.parseString(output.body().getValue());
    }

    @Override
    public void onDisconnect() {
        configRef = null;
    }

    /** {@inheritDoc} */
    @Override
    public Config config() {
        return configRef == null ? null : configRef.get();
    }
}
