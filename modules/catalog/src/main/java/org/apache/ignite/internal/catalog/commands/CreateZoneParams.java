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

import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * CREATE ZONE statement.
 */
public class CreateZoneParams extends AbstractZoneCommandParams {
    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Amount of zone partitions. */
    protected int partitions = DEFAULT_PARTITION_COUNT;

    /** Amount of zone partition replicas. */
    protected int replicas = DEFAULT_REPLICA_COUNT;

    /** Data nodes auto adjust timeout. */
    protected int dataNodesAutoAdjust = INFINITE_TIMER_VALUE;

    /** Data nodes auto adjust scale up timeout. */
    protected int dataNodesAutoAdjustScaleUp = INFINITE_TIMER_VALUE;

    /** Data nodes auto adjust scale down timeout. */
    protected int dataNodesAutoAdjustScaleDown = INFINITE_TIMER_VALUE;

    /** Nodes' filter. */
    protected String filter;

    /**
     * Returns amount of zone partitions.
     */
    public int partitions() {
        return partitions;
    }

    /**
     * Return amount of zone replicas.
     */
    public int replicas() {
        return replicas;
    }

    /**
     * Gets timeout in seconds between node added or node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust timeout.
     */
    public int dataNodesAutoAdjust() {
        return dataNodesAutoAdjust;
    }

    /**
     * Gets timeout in seconds between node added topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust scale up timeout.
     */
    public int dataNodesAutoAdjustScaleUp() {
        return dataNodesAutoAdjustScaleUp;
    }

    /**
     * Gets timeout in seconds between node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust scale down timeout.
     */
    public int dataNodesAutoAdjustScaleDown() {
        return dataNodesAutoAdjustScaleDown;
    }

    /**
     * Gets nodes' filter.
     *
     * @return Nodes' filter.
     */
    public String filter() {
        return filter;
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<CreateZoneParams, Builder> {
        Builder() {
            super(new CreateZoneParams());
        }


        /**
         * Sets amount of zone partitions.
         *
         * @param partitions Amount of partitions.
         */
        public Builder partitions(@Nullable Integer partitions) {
            params.partitions = Objects.requireNonNullElse(partitions, DEFAULT_PARTITION_COUNT);

            return this;
        }

        /**
         * Sets amount of zone replicas.
         *
         * @param replicas Amount of replicas.
         */
        public Builder replicas(@Nullable Integer replicas) {
            params.replicas = Objects.requireNonNullElse(replicas, DEFAULT_REPLICA_COUNT);

            return this;
        }

        /**
         * Sets timeout in seconds between node added or node left topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjust Timeout.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjust(@Nullable Integer dataNodesAutoAdjust) {
            params.dataNodesAutoAdjust = Objects.requireNonNullElse(dataNodesAutoAdjust, INFINITE_TIMER_VALUE);

            return this;
        }

        /**
         * Sets timeout in seconds between node added topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjustScaleUp Timeout.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjustScaleUp(@Nullable Integer dataNodesAutoAdjustScaleUp) {
            params.dataNodesAutoAdjustScaleUp = Objects.requireNonNullElse(dataNodesAutoAdjustScaleUp, INFINITE_TIMER_VALUE);

            return this;
        }

        /**
         * Sets timeout in seconds between node left topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself and data nodes switch.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjustScaleDown(@Nullable Integer dataNodesAutoAdjustScaleDown) {
            params.dataNodesAutoAdjustScaleDown = Objects.requireNonNullElse(dataNodesAutoAdjustScaleDown, INFINITE_TIMER_VALUE);

            return this;
        }

        /**
         * Sets nodes' filter.
         *
         * @param filter Nodes' filter.
         * @return This instance.
         */
        public Builder filter(String filter) {
            params.filter = filter;

            return this;
        }
    }
}
