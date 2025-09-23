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
import org.jetbrains.annotations.Nullable;

/**
 * Builder of a command that changes a particular zone.
 *
 * <p>A builder is considered to be reusable, thus implementation have
 * to make sure invocation of {@link #build()} method doesn't cause any
 * side effects on builder's state or any object created by the same builder.
 */
public interface AlterZoneCommandBuilder extends AbstractZoneCommandBuilder<AlterZoneCommandBuilder> {
    AlterZoneCommandBuilder ifExists(boolean ifExists);

    /**
     * Sets the number of partitions.
     *
     * @param partitions Optional number of partitions, it should be in the range from 1 to {@link CatalogUtils#MAX_PARTITION_COUNT}.
     * @return This instance.
     */
    AlterZoneCommandBuilder partitions(@Nullable Integer partitions);

    /**
     * Sets the number of replicas.
     *
     * @param replicas Optional number of replicas.
     * @return This instance.
     */
    AlterZoneCommandBuilder replicas(@Nullable Integer replicas);

    /**
     * Sets the quorum size.
     *
     * @param quorumSize Optional quorum size. It depends on the number of replicas and should be in the range from 1 to the
     *      {@code Math.round(replicas / 2.0) }.
     * @return This instance.
     */
    AlterZoneCommandBuilder quorumSize(@Nullable Integer quorumSize);

    /**
     * Sets timeout in seconds between node added topology event itself and data nodes switch.
     *
     * @param adjust Optional timeout.
     * @return This instance.
     */
    AlterZoneCommandBuilder dataNodesAutoAdjustScaleUp(@Nullable Integer adjust);

    /**
     * Sets timeout in seconds between node left topology event itself and data nodes switch.
     *
     * @param adjust Optional timeout in seconds between node left topology event itself and data nodes switch.
     * @return This instance.
     */
    AlterZoneCommandBuilder dataNodesAutoAdjustScaleDown(@Nullable Integer adjust);

    /**
     * Sets nodes filter.
     *
     * @param filter Optional nodes filter.
     * @return This instance.
     */
    AlterZoneCommandBuilder filter(@Nullable String filter);

    /**
     * Sets storage profiles.
     *
     * @param params Optional storage profiles params.
     * @return This instance.
     */
    AlterZoneCommandBuilder storageProfilesParams(@Nullable List<StorageProfileParams> params);
}
