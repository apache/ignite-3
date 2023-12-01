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
 * Builder of a command that creates a new zone.
 *
 * <p>A builder is considered to be reusable, thus implementation have
 * to make sure invocation of {@link #build()} method doesn't cause any
 * side effects on builder's state or any object created by the same builder.
 */
public interface CreateZoneCommandBuilder extends AbstractZoneCommandBuilder<CreateZoneCommandBuilder> {
    /**
     * Sets the number of partitions.
     *
     * @param partitions Number of partitions.
     * @return This instance.
     */
    CreateZoneCommandBuilder partitions(@Nullable Integer partitions);

    /**
     * Sets the number of replicas.
     *
     * @param replicas Number of replicas.
     * @return This instance.
     */
    CreateZoneCommandBuilder replicas(@Nullable Integer replicas);

    /**
     * Sets timeout in seconds between node added or node left topology event itself and data nodes switch.
     *
     * @param adjust Timeout.
     * @return This instance.
     */
    CreateZoneCommandBuilder dataNodesAutoAdjust(@Nullable Integer adjust);

    /**
     * Sets timeout in seconds between node added topology event itself and data nodes switch.
     *
     * @param adjust Timeout.
     * @return This instance.
     */
    CreateZoneCommandBuilder dataNodesAutoAdjustScaleUp(@Nullable Integer adjust);

    /**
     * Sets timeout in seconds between node left topology event itself and data nodes switch.
     *
     * @param adjust Timeout in seconds between node left topology event itself and data nodes switch.
     * @return This instance.
     */
    CreateZoneCommandBuilder dataNodesAutoAdjustScaleDown(@Nullable Integer adjust);

    /**
     * Sets nodes filter.
     *
     * @param filter Nodes filter.
     * @return This instance.
     */
    CreateZoneCommandBuilder filter(@Nullable String filter);

    /**
     * Sets the data storage.
     *
     * @param params Data storage.
     * @return This instance.
     */
    CreateZoneCommandBuilder dataStorageParams(@Nullable DataStorageParams params);
}
