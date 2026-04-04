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

package org.apache.ignite.internal.configuration.compatibility;

import static org.apache.ignite.internal.configuration.compatibility.ConfigurationCompatibilityTest.DEFAULT_FILE_NAME;
import static org.apache.ignite.internal.configuration.compatibility.ConfigurationCompatibilityTest.loadCurrentConfiguration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigShuttle;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationSnapshotManager;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Generates a snapshot for the current configuration.
 */
public class GenerateConfigurationSnapshot {
    private static final Path DEFAULT_SNAPSHOT_FILE = Path.of("modules", "runner", "build", "work", DEFAULT_FILE_NAME);

    private static final IgniteLogger LOG = Loggers.forClass(GenerateConfigurationSnapshot.class);

    /**
     * Generates a snapshot of the current configuration metadata and saves it to a file.
     */
    public static void main(String[] args) throws IOException {
        LOG.info("Passed arguments are ", Arrays.toString(args));
        List<ConfigNode> configNodes = loadCurrentConfiguration();

        ConfigShuttle shuttle = node -> LOG.info(node.toString());
        LOG.info("DUMP TREE:");
        configNodes.forEach(c -> c.accept(shuttle));

        Path outputPath = DEFAULT_SNAPSHOT_FILE;
        if (args.length > 0) {
            outputPath = Path.of(args[0]);
        }

        ConfigurationSnapshotManager.saveSnapshotToFile(configNodes, outputPath);

        LOG.info("Snapshot saved to: " + DEFAULT_SNAPSHOT_FILE.toAbsolutePath());
    }
}
