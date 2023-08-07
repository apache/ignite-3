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

package org.apache.ignite.internal.cli.core.repl.completer.unit;

import jakarta.inject.Singleton;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.cli.commands.cluster.unit.NodesAlias;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleter;
import org.apache.ignite.internal.cli.core.repl.completer.DynamicCompleterFactory;
import org.apache.ignite.internal.cli.core.repl.completer.StringDynamicCompleter;
import org.apache.ignite.internal.cli.core.repl.registry.NodeNameRegistry;

/** Factory for --nodes option completer. */
@Singleton
public class UnitNodesDynamicCompleterFactory implements DynamicCompleterFactory {

    private final NodeNameRegistry nodeNameRegistry;

    public UnitNodesDynamicCompleterFactory(NodeNameRegistry nodeNameRegistry) {
        this.nodeNameRegistry = nodeNameRegistry;
    }

    @Override
    public DynamicCompleter getDynamicCompleter(String[] words) {
        Set<String> values = new HashSet<>(nodeNameRegistry.names());
        for (NodesAlias alias : NodesAlias.values()) {
            values.add(alias.name());
        }

        return new StringDynamicCompleter(values);
    }
}
