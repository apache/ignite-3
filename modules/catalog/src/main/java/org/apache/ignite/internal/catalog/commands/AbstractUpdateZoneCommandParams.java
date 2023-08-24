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

/** Abstract class for creating/altering a distribution zone. */
abstract class AbstractUpdateZoneCommandParams extends AbstractZoneCommandParams {
    /** Constructor. */
    AbstractUpdateZoneCommandParams(String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjust,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable DataStorageParams dataStorage
    ) {
        super(zoneName);

        this.partitions = partitions;
        this.replicas = replicas;
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
        this.filter = filter;
        this.dataStorage = dataStorage;
    }

    private final @Nullable Integer partitions;

    private final @Nullable Integer replicas;

    private final @Nullable Integer dataNodesAutoAdjust;

    private final @Nullable Integer dataNodesAutoAdjustScaleUp;

    private final @Nullable Integer dataNodesAutoAdjustScaleDown;

    private final @Nullable String filter;

    private final @Nullable DataStorageParams dataStorage;

    /** Gets number of zone partitions, {@code null} if not set. */
    public @Nullable Integer partitions() {
        return partitions;
    }

    /** Gets number of zone replicas, {@code null} if not set. */
    public @Nullable Integer replicas() {
        return replicas;
    }

    /** Gets nodes filter, {@code null} if not set. */
    public @Nullable String filter() {
        return filter;
    }

    /** Gets timeout in seconds between node added or node left topology event itself and data nodes switch, {@code null} if not set. */
    public @Nullable Integer dataNodesAutoAdjust() {
        return dataNodesAutoAdjust;
    }

    /** Gets timeout in seconds between node added topology event itself and data nodes switch, {@code null} if not set. */
    public @Nullable Integer dataNodesAutoAdjustScaleUp() {
        return dataNodesAutoAdjustScaleUp;
    }

    /** Gets timeout in seconds between node left topology event itself and data nodes switch, {@code null} if not set. */
    public @Nullable Integer dataNodesAutoAdjustScaleDown() {
        return dataNodesAutoAdjustScaleDown;
    }

    /** Returns the data storage, {@code null} if not set. */
    public @Nullable DataStorageParams dataStorage() {
        return dataStorage;
    }

    /** Abstract parameters builder for creating/altering a distribution zone. */
    abstract static class AbstractUpdateZoneBuilder<ParamT extends AbstractUpdateZoneCommandParams, BuilderT> extends
            AbstractBuilder<ParamT, BuilderT> {
        protected @Nullable Integer partitions;

        protected @Nullable Integer replicas;

        protected @Nullable Integer dataNodesAutoAdjust;

        protected @Nullable Integer dataNodesAutoAdjustScaleUp;

        protected @Nullable Integer dataNodesAutoAdjustScaleDown;

        protected @Nullable String filter;

        protected @Nullable DataStorageParams dataStorage;

        /**
         * Sets the number of replicas.
         *
         * @param replicas Number of replicas.
         * @return This instance.
         */
        public BuilderT replicas(@Nullable Integer replicas) {
            this.replicas = replicas;

            return (BuilderT) this;
        }

        /**
         * Sets the number of partitions.
         *
         * @param partitions Number of partitions.
         * @return This instance.
         */
        public BuilderT partitions(@Nullable Integer partitions) {
            this.partitions = partitions;

            return (BuilderT) this;
        }

        /**
         * Sets timeout in seconds between node added or node left topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjust Timeout.
         * @return This instance.
         */
        public BuilderT dataNodesAutoAdjust(@Nullable Integer dataNodesAutoAdjust) {
            this.dataNodesAutoAdjust = dataNodesAutoAdjust;

            return (BuilderT) this;
        }

        /**
         * Sets timeout in seconds between node added topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjustScaleUp Timeout.
         * @return This instance.
         */
        public BuilderT dataNodesAutoAdjustScaleUp(@Nullable Integer dataNodesAutoAdjustScaleUp) {
            this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;

            return (BuilderT) this;
        }

        /**
         * Sets timeout in seconds between node left topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch.
         * @return This instance.
         */
        public BuilderT dataNodesAutoAdjustScaleDown(@Nullable Integer dataNodesAutoAdjustScaleDown) {
            this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;

            return (BuilderT) this;
        }

        /**
         * Sets nodes' filter.
         *
         * @param filter Nodes' filter.
         * @return This instance.
         */
        public BuilderT filter(@Nullable String filter) {
            this.filter = filter;

            return (BuilderT) this;
        }

        /**
         * Sets the data storage.
         *
         * @param dataStorage Data storage.
         * @return This instance.
         */
        public BuilderT dataStorage(@Nullable DataStorageParams dataStorage) {
            this.dataStorage = dataStorage;

            return (BuilderT) this;
        }
    }
}
