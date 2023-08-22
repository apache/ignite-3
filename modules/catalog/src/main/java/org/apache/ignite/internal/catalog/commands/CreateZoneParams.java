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

/** CREATE ZONE statement. */
public class CreateZoneParams extends AbstractUpdateZoneCommandParams {
    /** Constructor. */
    private CreateZoneParams(
            String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjust,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter,
            @Nullable DataStorageParams dataStorage
    ) {
        super(
                zoneName,
                partitions,
                replicas,
                dataNodesAutoAdjust,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown,
                filter,
                dataStorage
        );
    }

    /** Creates parameters builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Parameters builder. */
    public static class Builder extends AbstractUpdateZoneBuilder<CreateZoneParams, Builder> {
        @Override
        protected CreateZoneParams createParams() {
            return new CreateZoneParams(
                    zoneName,
                    partitions,
                    replicas,
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    filter,
                    dataStorage
            );
        }
    }
}
