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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import java.util.function.Predicate;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.Nullable;

/** Additional information used for mapping. */
public class MappingParameters {
    /** Empty mapping parameters. */
    public static final MappingParameters EMPTY = new MappingParameters(ArrayUtils.OBJECT_EMPTY_ARRAY, false, null);

    /** Allow map on backups. */
    public static final MappingParameters MAP_ON_BACKUPS = new MappingParameters(ArrayUtils.OBJECT_EMPTY_ARRAY, true, null);

    private final boolean mapOnBackups;
    private final Object[] dynamicParameters;
    private final @Nullable Predicate<String> nodeExclusionFilter;

    /**
     * Creates mapping parameters.
     *
     * @param dynamicParameters Dynamic parameters.
     * @param mapOnBackups Whether to use non-primary replicas for mapping or not.
     * @param nodeExclusionFilter If provided, all nodes which meet the predicate must be excluded from mapping.
     *
     * @return Mapping parameters.
     */
    public static MappingParameters create(
            Object[] dynamicParameters,
            boolean mapOnBackups,
            @Nullable Predicate<String> nodeExclusionFilter
    ) {
        if (dynamicParameters.length == 0 && nodeExclusionFilter == null) {
            return mapOnBackups ? MAP_ON_BACKUPS : EMPTY;
        } else {
            return new MappingParameters(dynamicParameters, mapOnBackups, nodeExclusionFilter);
        }
    }

    private MappingParameters(
            Object[] dynamicParameters,
            boolean mapOnBackups,
            @Nullable Predicate<String> nodeExclusionFilter
    ) {
        this.dynamicParameters = dynamicParameters;
        this.mapOnBackups = mapOnBackups;
        this.nodeExclusionFilter = nodeExclusionFilter;
    }

    /** Values of dynamic parameters. */
    Object[] dynamicParameters() {
        return dynamicParameters;
    }

    boolean mapOnBackups() {
        return mapOnBackups;
    }

    /** If returned, all nodes which meet the predicate must be excluded from mapping. */
    @Nullable Predicate<String> nodeExclusionFilter() {
        return nodeExclusionFilter;
    }
}
