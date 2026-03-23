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

import java.util.List;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.jetbrains.annotations.Nullable;

/**
 * Builder of a command that creates a new zone.
 *
 * <p>A builder is considered to be reusable, thus implementation have
 * to make sure invocation of {@link #build()} method doesn't cause any
 * side effects on builder's state or any object created by the same builder.
 */
public interface CreateZoneCommandBuilder extends AbstractZoneCommandBuilder<CreateZoneCommandBuilder> {
    CreateZoneCommandBuilder ifNotExists(boolean ifNotExists);

    /**
     * Sets the number of partitions.
     *
     * @param partitions Optional number of partitions, it should be in the range from 1 to {@link CatalogUtils#MAX_PARTITION_COUNT}.
     * @return This instance.
     */
    CreateZoneCommandBuilder partitions(@Nullable Integer partitions);

    /**
     * Sets the number of replicas.
     *
     * @param replicas Optional number of replicas.
     * @return This instance.
     */
    CreateZoneCommandBuilder replicas(@Nullable Integer replicas);

    /**
     * Sets the quorum size.
     *
     * @param quorumSize Optional quorum size, it should be in the range from 1 to the {@code Math.floor((replicas + 1)/2) }.
     * @return This instance.
     */
    CreateZoneCommandBuilder quorumSize(@Nullable Integer quorumSize);

    /**
     * Sets timeout in seconds between node added topology event itself and data nodes switch.
     *
     * @param adjust Optional timeout.
     * @return This instance.
     */
    CreateZoneCommandBuilder dataNodesAutoAdjustScaleUp(@Nullable Integer adjust);

    /**
     * Sets timeout in seconds between node left topology event itself and data nodes switch.
     *
     * @param adjust Optional timeout in seconds between node left topology event itself and data nodes switch.
     * @return This instance.
     */
    CreateZoneCommandBuilder dataNodesAutoAdjustScaleDown(@Nullable Integer adjust);

    /**
     * Sets nodes filter.
     *
     * @param filter Optional nodes filter.
     * @return This instance.
     */
    CreateZoneCommandBuilder filter(@Nullable String filter);

    /**
     * Sets the storage profiles.
     *
     * @param params Optional storage profile params.
     * @return This instance.
     */
    CreateZoneCommandBuilder storageProfilesParams(List<StorageProfileParams> params);

    /**
     * Sets consistency mode.
     *
     * @param params Optional consistency mode params.
     * @return This instance.
     */
    CreateZoneCommandBuilder consistencyModeParams(@Nullable ConsistencyMode params);
}
