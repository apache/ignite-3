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

package org.apache.ignite.internal.catalog;

import static java.util.Objects.requireNonNullElse;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateField;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateStorageProfileNames;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateZoneFilter;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;

import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Parameters container for {@link PartitionCountCalculator#calculate}.
 */
public final class PartitionCountCalculationParameters {
    private final String dataNodesFilter;

    private final List<String> storageProfiles;

    private final int replicaFactor;

    private PartitionCountCalculationParameters(
            String dataNodesFilter,
            List<String> storageProfiles,
            int replicaFactor
    ) {
        this.dataNodesFilter = dataNodesFilter;
        this.storageProfiles = storageProfiles;
        this.replicaFactor = replicaFactor;

        validate();
    }

    private void validate() {
        validateZoneFilter(dataNodesFilter);
        validateStorageProfileNames(storageProfiles);
        validateField(replicaFactor, 1, null, "Invalid number of replicas");
    }

    /** Returns data nodes filter. */
    public String dataNodesFilter() {
        return dataNodesFilter;
    }

    /** Returns storage profiles. */
    public List<String> storageProfiles() {
        return storageProfiles;
    }

    /** Returns replica factor. */
    public int replicaFactor() {
        return replicaFactor;
    }

    /** Creates new builder for parameters. */
    public static Builder builder() {
        return new Builder();
    }

    /** Parameters builder. */
    public static final class Builder {
        /** Data nodes filter, {@code null} if not set. */
        private @Nullable String dataNodesFilter;

        /** Storage profiles, {@code null} if not set. */
        private @Nullable List<String> storageProfiles;

        /** Replica factor, {@code null} if not set. */
        private @Nullable Integer replicaFactor;

        /**
         * Set data nodes filter.
         *
         * @param dataNodesFilter Data nodes filter.
         * @return {@code this}.
         */
        public Builder dataNodesFilter(@Nullable String dataNodesFilter) {
            this.dataNodesFilter = dataNodesFilter;
            return this;
        }

        /**
         * Set storage profiles.
         *
         * @param storageProfiles Storage profiles.
         * @return {@code this}.
         */
        public Builder storageProfiles(@Nullable List<String> storageProfiles) {
            this.storageProfiles = storageProfiles;
            return this;
        }

        /**
         * Set replica factor.
         *
         * @param replicaFactor Replica factor.
         * @return {@code this}.
         */
        public Builder replicaFactor(@Nullable Integer replicaFactor) {
            this.replicaFactor = replicaFactor;
            return this;
        }

        /** Builds parameters. */
        public PartitionCountCalculationParameters build() {
            return new PartitionCountCalculationParameters(
                    requireNonNullElse(dataNodesFilter, DEFAULT_FILTER),
                    requireNonNullElse(storageProfiles, List.of(DEFAULT_STORAGE_PROFILE)),
                    requireNonNullElse(replicaFactor, DEFAULT_REPLICA_COUNT)
            );
        }
    }
}
