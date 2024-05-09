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

package org.apache.ignite.internal.catalog.sql;

class WithOption extends QueryPart {
    private final String name;

    private final Object value;

    private WithOption(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    public static WithOption primaryZone(String zone) {
        return new WithOption("PRIMARY_ZONE", zone.toUpperCase());
    }

    public static WithOption partitions(Integer partitions) {
        return new WithOption("PARTITIONS", partitions);
    }

    public static WithOption replicas(Integer replicas) {
        return new WithOption("REPLICAS", replicas);
    }

    public static WithOption affinity(String affinity) {
        return new WithOption("AFFINITY_FUNCTION", affinity);
    }

    public static WithOption dataNodesAutoAdjust(Integer adjust) {
        return new WithOption("DATA_NODES_AUTO_ADJUST", adjust);
    }

    public static WithOption dataNodesAutoAdjustScaleUp(Integer adjust) {
        return new WithOption("DATA_NODES_AUTO_ADJUST_SCALE_UP", adjust);
    }

    public static WithOption dataNodesAutoAdjustScaleDown(Integer adjust) {
        return new WithOption("DATA_NODES_AUTO_ADJUST_SCALE_DOWN", adjust);
    }

    public static WithOption filter(String filter) {
        return new WithOption("DATA_NODES_FILTER", filter);
    }

    public static WithOption dataRegion(String dataRegion) {
        return new WithOption("DATAREGION", dataRegion);
    }

    public static WithOption storageProfiles(String storageProfiles) {
        return new WithOption("STORAGE_PROFILES", storageProfiles);
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql(name).sql("=");
        boolean isNeedsQuotes = value instanceof String;
        if (isNeedsQuotes) {
            ctx.sql("'").sql(value.toString()).sql("'");
        } else {
            ctx.sql(value.toString());
        }
    }
}
