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

package org.apache.ignite.internal.distributionzones;

import org.jetbrains.annotations.Nullable;

/**
 * Distribution zone configuration.
 */
public class DistributionZoneConfigurationParameters {
    /** Zone name. */
    private final String name;

    /** Data nodes auto adjust timeout. */
    private final Integer dataNodesAutoAdjust;

    /** Data nodes auto adjust scale up timeout. */
    private final Integer dataNodesAutoAdjustScaleUp;

    /** Data nodes auto adjust scale down timeout. */
    private final Integer dataNodesAutoAdjustScaleDown;

    /**
     * The constructor.
     */
    private DistributionZoneConfigurationParameters(
            String name,
            Integer dataNodesAutoAdjust,
            Integer dataNodesAutoAdjustScaleUp,
            Integer dataNodesAutoAdjustScaleDown
    ) {
        this.name = name;
        this.dataNodesAutoAdjust = dataNodesAutoAdjust;
        this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
        this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
    }

    /**
     * Gets the zone name.
     *
     * @return The zone name.
     */
    public String name() {
        return name;
    }

    /**
     * Gets timeout in seconds between node added or node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust timeout.
     */
    @Nullable public Integer dataNodesAutoAdjust() {
        return dataNodesAutoAdjust;
    }

    /**
     * Gets timeout in seconds between node added topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust scale up timeout.
     */
    @Nullable public Integer dataNodesAutoAdjustScaleUp() {
        return dataNodesAutoAdjustScaleUp;
    }

    /**
     * Gets timeout in seconds between node left topology event itself and data nodes switch.
     *
     * @return Data nodes auto adjust scale down timeout.
     */
    @Nullable public Integer dataNodesAutoAdjustScaleDown() {
        return dataNodesAutoAdjustScaleDown;
    }

    /**
     * Builder for distribution zone configuration.
     */
    public static class Builder {
        /** Zone name. */
        private String name;

        /** Data nodes auto adjust timeout. */
        private Integer dataNodesAutoAdjust;

        /** Data nodes auto adjust scale up timeout. */
        private Integer dataNodesAutoAdjustScaleUp;

        /** Data nodes auto adjust scale down timeout. */
        private Integer dataNodesAutoAdjustScaleDown;

        /**
         * Constructor.
         *
         * @param name Name.
         */
        public Builder(String name) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Illegal distribution zone name [name=" + name + ']');
            }

            this.name = name;
        }

        /**
         * Sets timeout in seconds between node added or node left topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjust Timeout.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjust(int dataNodesAutoAdjust) {
            this.dataNodesAutoAdjust = dataNodesAutoAdjust;

            return this;
        }

        /**
         * Sets timeout in seconds between node added topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjustScaleUp Timeout.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjustScaleUp(int dataNodesAutoAdjustScaleUp) {
            this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;

            return this;
        }

        /**
         * Sets timeout in seconds between node left topology event itself and data nodes switch.
         *
         * @param dataNodesAutoAdjustScaleDown Timeout in seconds between node left topology event itself
         *     and data nodes switch.
         * @return This instance.
         */
        public Builder dataNodesAutoAdjustScaleDown(int dataNodesAutoAdjustScaleDown) {
            this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;

            return this;
        }

        /**
         * Builds the distribution zone configuration.
         *
         * @return Distribution zone configuration.
         */
        public DistributionZoneConfigurationParameters build() {
            if (dataNodesAutoAdjust != null
                    && (dataNodesAutoAdjustScaleUp != null || dataNodesAutoAdjustScaleDown != null)
            ) {
                throw new IllegalArgumentException(
                        "Not compatible parameters [dataNodesAutoAdjust=" + dataNodesAutoAdjust
                                + ", dataNodesAutoAdjustScaleUp=" + dataNodesAutoAdjustScaleUp
                                + ", dataNodesAutoAdjustScaleDown=" + dataNodesAutoAdjustScaleDown + ']'
                );
            }

            return new DistributionZoneConfigurationParameters(
                    name,
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown
            );
        }
    }
}
