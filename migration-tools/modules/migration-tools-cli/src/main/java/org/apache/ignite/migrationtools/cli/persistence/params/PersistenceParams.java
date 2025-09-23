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

package org.apache.ignite.migrationtools.cli.persistence.params;

import java.nio.file.Path;
import picocli.CommandLine;

/** Persistence Command parameters. */
public class PersistenceParams {
    @CommandLine.Parameters(paramLabel = "work-dir", description = "Work Directory of one or many Ignite 2 nodes")
    private Path workDir;
    @CommandLine.Parameters(paramLabel = "consistent-id", description = "Consistent ID of the Ignite 2 node")
    private String nodeConsistentId;
    @CommandLine.Parameters(paramLabel = "config-file", description = "Ignite 2 Configuration XML")
    private Path configFile;

    public Path workDir() {
        return workDir;
    }

    public String nodeConsistentId() {
        return nodeConsistentId;
    }

    public Path configFile() {
        return configFile;
    }
}
