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

import picocli.CommandLine;

/** Parameter for the Retriable Migrate Cache command. */
public class RetrieableMigrateCacheParams {
    @CommandLine.Option(names = {"--retryLimit"}, defaultValue = "0",
            description = "Retries the migration up to N times on retrievable errors. 0 (Default) does not retry."
                    + " Implies save progress is not disabled.")
    private int retryLimit;

    @CommandLine.Option(names = {"--retryBackoff"}, defaultValue = "0",
            description = "Waits N seconds before retry the next attempt at migration the cache. Default: 0 (retry immediately).")
    private int retryBackoffSeconds;

    private RetrieableMigrateCacheParams() {
        // Intentionally left blank.
    }

    public RetrieableMigrateCacheParams(int retryLimit, int retryBackoffSeconds) {
        this.retryLimit = retryLimit;
        this.retryBackoffSeconds = retryBackoffSeconds;
    }

    public int retryLimit() {
        return retryLimit;
    }

    public int retryBackoffSeconds() {
        return retryBackoffSeconds;
    }
}
