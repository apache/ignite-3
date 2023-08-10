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
import org.apache.ignite.internal.cli.call.configuration.NodeConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigShowCallInput;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.repl.registry.NodeConfigRegistry;
import org.apache.ignite.internal.cli.event.ConnectionEventListener;

/** Implementation of {@link NodeConfigRegistry}. */
@Singleton
public class NodeConfigRegistryImpl implements NodeConfigRegistry, ConnectionEventListener {

    private final NodeConfigShowCall nodeConfigShowCall;

    private LazyObjectRef<Config> configRef;

    public NodeConfigRegistryImpl(NodeConfigShowCall nodeConfigShowCall) {
        this.nodeConfigShowCall = nodeConfigShowCall;
    }

    @Override
    public void onConnect(SessionInfo sessionInfo) {
        configRef = new LazyObjectRef<>(() -> fetchConfig(sessionInfo));
    }

    private Config fetchConfig(SessionInfo sessionInfo) {
        return ConfigFactory.parseString(
                nodeConfigShowCall.execute(
                        // todo https://issues.apache.org/jira/browse/IGNITE-17416
                        NodeConfigShowCallInput.builder().nodeUrl(sessionInfo.nodeUrl()).build()
                ).body().getValue());
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
