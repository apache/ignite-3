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

package org.apache.ignite.catalog.definitions;

import java.util.Objects;

/**
 * Definition of the {@code CREATE ZONE} statement.
 */
public class ZoneDefinition {
    private final String zoneName;

    private final boolean ifNotExists;

    private final Integer partitions;

    private final Integer replicas;

    private final String storageProfiles;


    private ZoneDefinition(
            String zoneName,
            boolean ifNotExists,
            Integer partitions,
            Integer replicas,
            String storageProfiles
    ) {
        this.zoneName = zoneName;
        this.ifNotExists = ifNotExists;
        this.partitions = partitions;
        this.replicas = replicas;
        this.storageProfiles = storageProfiles;
    }

    /**
     * Creates a builder for the zone with the specified name.
     *
     * @param zoneName Zone name.
     * @return Builder.
     */
    public static Builder builder(String zoneName) {
        return new Builder().zoneName(zoneName);
    }

    /**
     * Returns zone name.
     *
     * @return Zone name.
     */
    public String zoneName() {
        return zoneName;
    }

    /**
     * Returns not exists flag.
     *
     * @return {@code true} if {@code IF NOT EXISTS} clause should be added to the statement.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * Returns number of partitions.
     *
     * @return Number of partitions.
     */
    public Integer partitions() {
        return partitions;
    }

    /**
     * Returns number of replicas.
     *
     * @return Number of replicas.
     */
    public Integer replicas() {
        return replicas;
    }

    /**
     * Returns storage profiles.
     *
     * @return Storage profiles.
     */
    public String storageProfiles() {
        return storageProfiles;
    }

    /**
     * Returns new builder using this definition.
     *
     * @return New builder.
     */
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Builder for the zone definition.
     */
    public static class Builder {
        private String zoneName;

        private boolean ifNotExists;

        private Integer partitions;

        private Integer replicas;

        private String storageProfiles;

        private Builder() {}

        private Builder(ZoneDefinition definition) {
            zoneName = definition.zoneName;
            ifNotExists = definition.ifNotExists;
            partitions = definition.partitions;
            replicas = definition.replicas;
            storageProfiles = definition.storageProfiles;
        }

        /**
         * Sets the zone name.
         *
         * @param zoneName Zone name.
         * @return This builder instance.
         */
        Builder zoneName(String zoneName) {
            this.zoneName = zoneName;
            return this;
        }

        /**
         * Sets the not exists flag.
         *
         * @return This builder instance.
         */
        public Builder ifNotExists() {
            this.ifNotExists = true;
            return this;
        }

        /**
         * Sets the number of partitions.
         *
         * @param partitions Number of partitions.
         * @return This builder instance.
         */
        public Builder partitions(Integer partitions) {
            Objects.requireNonNull(partitions, "Number of partitions must not be null.");

            this.partitions = partitions;
            return this;
        }

        /**
         * Sets the number of replicas.
         *
         * @param replicas Number of replicas.
         * @return This builder instance.
         */
        public Builder replicas(Integer replicas) {
            Objects.requireNonNull(replicas, "Number of replicas must not be null.");

            this.replicas = replicas;
            return this;
        }

        /**
         * Sets the storage profiles.
         *
         * @param storageProfiles Storage profiles.
         * @return This builder instance.
         */
        public Builder storageProfiles(String storageProfiles) {
            Objects.requireNonNull(storageProfiles, "Storage profiles must not be null.");

            this.storageProfiles = storageProfiles;
            return this;
        }

        /**
         * Builds the zone definition.
         *
         * @return Zone definition.
         */
        public ZoneDefinition build() {
            Objects.requireNonNull(zoneName, "Zone name must not be null.");
            if (zoneName.isBlank()) {
                throw new IllegalArgumentException("Zone name must not be blank.");
            }

            return new ZoneDefinition(
                    zoneName,
                    ifNotExists,
                    partitions,
                    replicas,
                    storageProfiles
            );
        }
    }
}
