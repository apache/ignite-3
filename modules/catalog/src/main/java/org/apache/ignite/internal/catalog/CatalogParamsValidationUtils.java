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

package org.apache.ignite.internal.catalog;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.lang.ErrorGroups.DistributionZones;
import org.apache.ignite.lang.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for validating catalog commands parameters.
 */
class CatalogParamsValidationUtils {
    static void validateCreateZoneParams(CreateZoneParams params) {
        validateUpdateZoneFieldsParameters(
                params.zoneName(),
                params.partitions(),
                params.replicas(),
                params.dataNodesAutoAdjust(),
                params.dataNodesAutoAdjustScaleUp(),
                params.dataNodesAutoAdjustScaleDown(),
                params.filter()
        );
    }

    static void validateAlterZoneParams(AlterZoneParams params) {
        validateUpdateZoneFieldsParameters(
                params.zoneName(),
                params.partitions(),
                params.replicas(),
                params.dataNodesAutoAdjust(),
                params.dataNodesAutoAdjustScaleUp(),
                params.dataNodesAutoAdjustScaleDown(),
                params.filter()
        );
    }

    static void validateDropZoneParams(DropZoneParams params) {
        validateZoneName(params.zoneName());
    }

    static void validateRenameZoneParams(RenameZoneParams params) {
        validateZoneName(params.zoneName());
        validateZoneName(params.newZoneName(), "Missing new zone name");
    }

    private static void validateUpdateZoneFieldsParameters(
            String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjust,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter
    ) {
        validateZoneName(zoneName);

        validateZonePartitions(partitions);
        validateZoneReplicas(replicas);

        validateZoneDataNodesAutoAdjust(dataNodesAutoAdjust);
        validateZoneDataNodesAutoAdjustScaleUp(dataNodesAutoAdjustScaleUp);
        validateZoneDataNodesAutoAdjustScaleDown(dataNodesAutoAdjustScaleDown);

        validateZoneDataNodesAutoAdjustParametersCompatibility(
                dataNodesAutoAdjust,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown
        );

        validateZoneFilter(filter);
    }

    private static void validateZoneName(String zoneName) {
        validateZoneName(zoneName, "Missing zone name");
    }

    private static void validateZoneName(String zoneName, String errorMessage) {
        if (StringUtils.nullOrBlank(zoneName)) {
            throw new CatalogValidationException(
                    DistributionZones.ZONE_DEFINITION_ERR,
                    errorMessage
            );
        }
    }

    private static void validateZonePartitions(@Nullable Integer partitions) {
        validateZoneField(partitions, 1, MAX_PARTITION_COUNT, "Invalid number of partitions");
    }

    private static void validateZoneReplicas(@Nullable Integer replicas) {
        validateZoneField(replicas, 1, null, "Invalid number of replicas");
    }

    private static void validateZoneDataNodesAutoAdjust(@Nullable Integer dataNodesAutoAdjust) {
        validateZoneField(dataNodesAutoAdjust, 0, null, "Invalid data nodes auto adjust");
    }

    private static void validateZoneDataNodesAutoAdjustScaleUp(@Nullable Integer dataNodesAutoAdjustScaleUp) {
        validateZoneField(dataNodesAutoAdjustScaleUp, 0, null, "Invalid data nodes auto adjust scale up");
    }

    private static void validateZoneDataNodesAutoAdjustScaleDown(@Nullable Integer dataNodesAutoAdjustScaleDown) {
        validateZoneField(dataNodesAutoAdjustScaleDown, 0, null, "Invalid data nodes auto adjust scale down");
    }

    static void validateZoneDataNodesAutoAdjustParametersCompatibility(
            @Nullable Integer dataNodesAutoAdjust,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown
    ) {
        if (dataNodesAutoAdjust == null || dataNodesAutoAdjust == INFINITE_TIMER_VALUE) {
            return;
        }

        if ((dataNodesAutoAdjustScaleUp != null && dataNodesAutoAdjustScaleUp != INFINITE_TIMER_VALUE)
                || (dataNodesAutoAdjustScaleDown != null && dataNodesAutoAdjustScaleDown != INFINITE_TIMER_VALUE)) {
            throw new CatalogValidationException(
                    DistributionZones.ZONE_DEFINITION_ERR,
                    "Not compatible parameters [dataNodesAutoAdjust={}, dataNodesAutoAdjustScaleUp={}, dataNodesAutoAdjustScaleDown={}]",
                    dataNodesAutoAdjust, dataNodesAutoAdjustScaleUp, dataNodesAutoAdjustScaleDown
            );
        }
    }

    private static void validateZoneFilter(@Nullable String filter) {
        if (filter == null) {
            return;
        }

        try {
            JsonPath.compile(filter);
        } catch (InvalidPathException e) {
            String error = e.getMessage() == null ? "Unknown JsonPath compilation error." : e.getMessage();

            throw new CatalogValidationException(
                    DistributionZones.ZONE_DEFINITION_ERR,
                    "Invalid filter: [value={}, error={}]",
                    e,
                    filter, error
            );
        }
    }

    private static void validateZoneField(@Nullable Integer value, int min, @Nullable Integer max, String errorPrefix) {
        if (value == null) {
            return;
        }

        if (value < min || (max != null && value > max)) {
            throw new CatalogValidationException(
                    DistributionZones.ZONE_DEFINITION_ERR,
                    "{}: [value={}, min={}" + (max == null ? ']' : ", max={}]"),
                    errorPrefix, value, min, max
            );
        }
    }
}
