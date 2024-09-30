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

package org.apache.ignite.internal.cli.call.recovery.cluster;

import java.util.Objects;
import org.apache.ignite.internal.cli.commands.recovery.cluster.migrate.MigrateToClusterMixin;
import org.apache.ignite.internal.cli.core.call.CallInput;

/** Input for the {@link MigrateToClusterCall} call. */
public class MigrateToClusterCallInput implements CallInput {
    private final String oldClusterUrl;
    private final String newClusterUrl;

    /** Old cluster url. */
    public String oldClusterUrl() {
        return oldClusterUrl;
    }

    /** New cluster url. */
    public String newClusterUrl() {
        return newClusterUrl;
    }

    private MigrateToClusterCallInput(String oldClusterUrl, String newClusterUrl) {
        Objects.requireNonNull(oldClusterUrl);
        Objects.requireNonNull(newClusterUrl);

        this.oldClusterUrl = oldClusterUrl;
        this.newClusterUrl = newClusterUrl;
    }

    /** Returns {@link MigrateToClusterCallInput} with specified arguments. */
    public static MigrateToClusterCallInput of(MigrateToClusterMixin statesArgs) {
        return builder()
                .oldClusterUrl(statesArgs.getOldClusterUrl())
                .newClusterUrl(statesArgs.getNewClusterUrl())
                .build();
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link MigrateToClusterCallInput}.
     */
    private static MigrateToClusterCallInputBuilder builder() {
        return new MigrateToClusterCallInputBuilder();
    }

    /** Builder for {@link MigrateToClusterCallInput}. */
    private static class MigrateToClusterCallInputBuilder {
        private String oldClusterUrl;
        private String newClusterUrl;

        /** Sets old cluster URL. */
        MigrateToClusterCallInputBuilder oldClusterUrl(String oldClusterUrl) {
            this.oldClusterUrl = oldClusterUrl;
            return this;
        }

        /** Sets new cluster URL. */
        MigrateToClusterCallInputBuilder newClusterUrl(String newClusterUrl) {
            this.newClusterUrl = newClusterUrl;
            return this;
        }

        /** Builds {@link MigrateToClusterCallInput}. */
        MigrateToClusterCallInput build() {
            return new MigrateToClusterCallInput(oldClusterUrl, newClusterUrl);
        }
    }
}
