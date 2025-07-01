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

package org.apache.ignite.internal.configuration.compatibility.framework;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

/**
 * Compares two configuration trees (snapshot and current).
 */
public class ConfigurationTreeComparator {
    /**
     * Validates the current configuration tree is compatible with the snapshot.
     */
    public static void ensureCompatible(List<ConfigNode> snapshot, List<ConfigNode> current) {
        // TODO: add more sophisticated comparison logic here
        String currentDump = dumpTree(current);
        String snapshotDump = dumpTree(snapshot);
        assertEquals(currentDump, snapshotDump, "Configuration metadata is incompatible");
    }

    /**
     * Compares the configuration trees are equals by dumping their state to string.
     */
    public static void compare(List<ConfigNode> tree1, List<ConfigNode> tree2) {
        String dump1 = dumpTree(tree1);
        String dump2 = dumpTree(tree2);
        assertEquals(dump1, dump2, "Configuration metadata mismatch");
    }

    private static String dumpTree(List<ConfigNode> nodes) {
        DumpingShuttle shuttle = new DumpingShuttle();
        nodes.forEach(n -> n.accept(shuttle));
        return shuttle.toString();
    }

    /**
     * Configuration tree visitor that dumps tree state to string.
     */
    private static class DumpingShuttle implements ConfigShuttle {
        private final StringBuilder sb = new StringBuilder();

        @Override
        public void visit(ConfigNode node) {
            sb.append(node.toString());
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }
}
