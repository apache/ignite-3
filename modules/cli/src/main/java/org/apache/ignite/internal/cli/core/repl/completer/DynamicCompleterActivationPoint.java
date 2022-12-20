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

package org.apache.ignite.internal.cli.core.repl.completer;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.commands.Options;
import org.apache.ignite.internal.cli.core.repl.completer.filter.ExclusionsCompleterFilter;
import org.apache.ignite.internal.cli.core.repl.completer.hocon.ClusterConfigDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.hocon.NodeConfigDynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.node.NodeNameDynamicCompleterFactory;

/** Activation point that links commands with dynamic completers. */
@Singleton
public class DynamicCompleterActivationPoint {

    private final NodeNameDynamicCompleterFactory nodeNameDynamicCompleterFactory;
    private final ClusterConfigDynamicCompleterFactory clusterConfigDynamicCompleterFactory;
    private final NodeConfigDynamicCompleterFactory nodeConfigDynamicCompleterFactory;

    /** Main constructor. */
    public DynamicCompleterActivationPoint(
            NodeNameDynamicCompleterFactory nodeNameDynamicCompleterFactory,
            ClusterConfigDynamicCompleterFactory clusterConfigDynamicCompleterFactory,
            NodeConfigDynamicCompleterFactory nodeConfigDynamicCompleterFactory
    ) {
        this.nodeNameDynamicCompleterFactory = nodeNameDynamicCompleterFactory;
        this.clusterConfigDynamicCompleterFactory = clusterConfigDynamicCompleterFactory;
        this.nodeConfigDynamicCompleterFactory = nodeConfigDynamicCompleterFactory;
    }


    /**
     * Registers all dynamic completers in given {@link DynamicCompleterRegistry}.
     */
    public void activateDynamicCompleter(DynamicCompleterRegistry registry) {
        registry.register(
                CompleterConf.builder()
                        .command("cluster", "config", "show")
                        .command("cluster", "config", "update").build(),
                clusterConfigDynamicCompleterFactory
        );
        registry.register(
                CompleterConf.builder()
                        .command("node", "config", "show").build(),
                nodeConfigDynamicCompleterFactory
        );

        registry.register(
                CompleterConf.builder()
                        .command("node", "config", "update")
                        .filter(new ExclusionsCompleterFilter("compute", "raft"))
                        .build(),
                nodeConfigDynamicCompleterFactory
        );

        // exclusive option that disables other completers for node name
        registry.register(
                CompleterConf.builder()
                        .enableOptions(Options.NODE_NAME)
                        .exclusiveEnableOptions().build(),
                nodeNameDynamicCompleterFactory
        );
        registry.register(
                CompleterConf.forCommand("connect"),
                nodeNameDynamicCompleterFactory
        );
        registry.register(
                CompleterConf.builder()
                        .command("cluster", "init")
                        .enableOptions(Options.META_STORAGE_NODE_NAME, Options.CMG_NODE_NAME)
                        .build(),
                nodeNameDynamicCompleterFactory
        );
    }
}
