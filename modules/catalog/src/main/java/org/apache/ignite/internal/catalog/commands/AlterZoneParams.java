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

package org.apache.ignite.internal.catalog.commands;

import org.jetbrains.annotations.Nullable;

/**
 * ALTER ZONE SET statement.
 */
public class AlterZoneParams extends AbstractZoneCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Number of zone replicas. */
    private @Nullable Integer replicas;

    /** Number of zone partitions. */
    private @Nullable Integer partitions;

    /** Data nodes auto adjust timeout. */
    private @Nullable Integer dataNodesAutoAdjust;

    /** Data nodes auto adjust scale up timeout. */
    private @Nullable Integer dataNodesAutoAdjustScaleUp;

    /** Data nodes auto adjust scale down timeout. */
    private @Nullable Integer dataNodesAutoAdjustScaleDown;

    /* Nodes' filter. */
    private @Nullable String filter;

    /** Data storage. */
    private @Nullable DataStorageParams dataStorage;

    /**
     * Gets number of zone replicas.
     *
     * @return Number of zone replicas.
     */
    public @Nullable Integer replicas() {
        return replicas;
    }

    /**
     * Gets number of zone partitions.
     *
     * @return Number of zone partitions.
     */
    public @Nullable Integer partitions() {
        return partitions;
    }

    /**
     * Gets nodes' filter.
     *
     * @return Nodes' filter.
     */
    public @Nullable String filter() {
        return filter;
    }

    /**
     * Gets timeout in seconds between node added or node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust timeout.
     */
    public @Nullable Integer dataNodesAutoAdjust() {
        return dataNodesAutoAdjust;
    }

    /**
     * Gets timeout in seconds between node added topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust scale up timeout.
     */
    public @Nullable Integer dataNodesAutoAdjustScaleUp() {
        return dataNodesAutoAdjustScaleUp;
    }

    /**
     * Gets timeout in seconds between node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust scale down timeout.
     */
    public @Nullable Integer dataNodesAutoAdjustScaleDown() {
        return dataNodesAutoAdjustScaleDown;
    }

    /** Returns the data storage, {@code null} if not set. */
    public @Nullable DataStorageParams dataStorage() {
        return dataStorage;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<AlterZoneParams, Builder> {
        Builder() {
            super(new AlterZoneParams());
        }

        /**
         * Sets the number of replicas.
         *
         * @param replicas Number of replicas.
         * @return This instance.
         */
        public Builder replicas(@Nullable Integer replicas) {
            params.replicas = replicas;

            return this;
        }

        /**
         * Sets the number of partitions.
         *
         * @param partitions Number of partitions.
         * @return This instance.
         */
        public Builder partitions(@Nullable Integer partitions) {
            params.partitions = partitions;

            return this;
        }

        /**
         * Sets timeout in seconds between node added or node left topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjust Timeout.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjust(@Nullable Integer dataNodesAutoAdjust) {
            params.dataNodesAutoAdjust = dataNodesAutoAdjust;

            return this;
        }

        /**
         * Sets timeout in seconds between node added topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjustScaleUp Timeout.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjustScaleUp(@Nullable Integer dataNodesAutoAdjustScaleUp) {
            params.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;

            return this;
        }

        /**
         * Sets timeout in seconds between node left topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjustScaleDown(@Nullable Integer dataNodesAutoAdjustScaleDown) {
            params.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;

            return this;
        }

        /**
         * Sets nodes' filter.
         *
         * @param filter Nodes' filter.
         * @return This instance.
         */
        public Builder filter(@Nullable String filter) {
            params.filter = filter;

            return this;
        }

        /**
         * Sets the data storage.
         *
         * @param dataStorage Data storage.
         * @return This instance.
         */
        public Builder dataStorage(@Nullable DataStorageParams dataStorage) {
            params.dataStorage = dataStorage;

            return this;
        }
    }
}
