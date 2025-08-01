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
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;

/** MigrateCacheParams. */
public class MigrateCacheParams {
    @CommandLine.Parameters(paramLabel = "cacheName")
    private String cacheName;

    @CommandLine.Parameters(paramLabel = "urls", arity = "1..*", defaultValue = "127.0.0.1", description = "URLs to connect to the cluster")
    private String[] addresses;

    @CommandLine.Option(names = "--mode", description = "Mapping error handling policy: ${COMPLETION-CANDIDATES}", defaultValue = "ABORT")
    private MigrationMode migrationMode;

    @CommandLine.Option(names = {"--rate-limiter"}, defaultValue = "-1", description = "Limits the number of migrated records per second."
            + " Uses a very basic rate limiter implementation, may be prone to bursts.")
    private int rateLimiter;

    @CommandLine.Option(names = {"--no-save-progress"}, description = "Disables saving a progress file at the end of the run.")
    private boolean saveProgressFileDisabled;

    @Nullable
    @CommandLine.Option(names = {"--resume-from"}, description = "Resumes the migration based on the progress file provided.")
    private Path progressFileToRead;

    private MigrateCacheParams() {
        // Intentionally left blank.
    }

    /**
     * Constructor.
     *
     * @param cacheName Cache Name.
     * @param addresses Address.
     * @param migrationMode Migration Mode.
     * @param rateLimiter Rate Limiter
     * @param saveProgressFileDisabled Save Progress File Enabled.
     * @param progressFileToRead Progress file to read.
     */
    public MigrateCacheParams(
            String cacheName,
            String[] addresses,
            MigrationMode migrationMode,
            int rateLimiter,
            boolean saveProgressFileDisabled,
            @Nullable Path progressFileToRead) {
        this.cacheName = cacheName;
        this.addresses = addresses;
        this.migrationMode = migrationMode;
        this.rateLimiter = rateLimiter;
        this.saveProgressFileDisabled = saveProgressFileDisabled;
        this.progressFileToRead = progressFileToRead;
    }

    public String cacheName() {
        return cacheName;
    }

    public String[] addresses() {
        return addresses;
    }

    public MigrationMode migrationMode() {
        return migrationMode;
    }

    public int rateLimiter() {
        return rateLimiter;
    }

    public boolean saveProgressFileDisabled() {
        return saveProgressFileDisabled;
    }

    public Path progressFileToRead() {
        return progressFileToRead;
    }
}
