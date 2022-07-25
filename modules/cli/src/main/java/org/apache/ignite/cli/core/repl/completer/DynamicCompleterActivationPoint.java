/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.core.repl.completer;

import jakarta.inject.Singleton;

/**
 * Activation point that links commands with dynamic completers.
 */
@Singleton
public class DynamicCompleterActivationPoint {

    private final DynamicCompleterFactory factory;

    /** Default constructor. */
    public DynamicCompleterActivationPoint(DynamicCompleterFactory factory) {
        this.factory = factory;
    }

    /**
     * Registers all dynamic completers in given {@link DynamicCompleterRegistry}.
     */
    public void activateDynamicCompleter(DynamicCompleterRegistry registry) {
        registry.register(new String[]{"cluster", "config", "show"}, factory.clusterConfigCompleter("--selector"));
        registry.register(new String[]{"cluster", "config", "update"}, factory.clusterConfigCompleter(""));
        registry.register(new String[]{"node", "config", "show"}, factory.nodeConfigCompleter("--selector"));
        registry.register(new String[]{"node", "config", "update"}, factory.nodeConfigCompleter(""));
    }
}
