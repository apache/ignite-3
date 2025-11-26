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

package org.apache.ignite.catalog.annotations;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Describes a distribution zone.
 */
@Target({})
@Retention(RUNTIME)
public @interface Zone {
    /**
     * The name of the zone. If it's empty, the name of the class annotated with this annotation will be used.
     *
     * @return The name of the zone.
     */
    String value();

    /**
     * Storage profiles.
     *
     * @return Storage profiles.
     */
    String storageProfiles();

    /**
     * Number of partitions.
     *
     * @return Number of partitions.
     */
    int partitions() default -1;

    /**
     * Number of replicas.
     *
     * @return Number of replicas.
     */
    int replicas() default -1;

    /**
     * Quorum size.
     *
     * @return Quorum size.
     */
    int quorumSize() default -1;

    /**
     * Distribution algorithm.
     *
     * @return Distribution algorithm.
     */
    String distributionAlgorithm() default "";

    /**
     * Deprecated, should not be used anymore. Timeout in seconds between node added or node left topology event itself and data nodes
     * switch.
     *
     * @return Timeout.
     */
    @Deprecated
    int dataNodesAutoAdjust() default -1;

    /**
     * Timeout in seconds between node added topology event itself and data nodes switch.
     *
     * @return Timeout.
     */
    int dataNodesAutoAdjustScaleUp() default -1;

    /**
     * Timeout in seconds between node left topology event itself and data nodes switch.
     *
     * @return Timeout.
     */
    int dataNodesAutoAdjustScaleDown() default -1;

    /**
     * Nodes filter.
     *
     * @return Nodes filter.
     */
    String filter() default "";

    /**
     * Consistency mode.
     *
     * @return Consistency mode.
     */
    String consistencyMode() default "";
}
