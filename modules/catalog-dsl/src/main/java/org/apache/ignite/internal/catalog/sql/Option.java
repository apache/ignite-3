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

class Option extends QueryPart {
    private final String name;

    private final Object value;

    private Option(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    public static Option partitions(Integer partitions) {
        return new Option("PARTITIONS", partitions);
    }

    public static Option replicas(Integer replicas) {
        return new Option("REPLICAS", replicas);
    }

    public static Option quorumSize(Integer quorumSize) {
        return new Option("QUORUM_SIZE", quorumSize);
    }

    public static Option dataNodesAutoAdjustScaleUp(Integer adjust) {
        return new Option("DATA_NODES_AUTO_ADJUST_SCALE_UP", adjust);
    }

    public static Option dataNodesAutoAdjustScaleDown(Integer adjust) {
        return new Option("DATA_NODES_AUTO_ADJUST_SCALE_DOWN", adjust);
    }

    public static Option distributionAlgorithm(String distributionAlgorithm) {
        return new Option("DISTRIBUTION_ALGORITHM", distributionAlgorithm);
    }

    public static Option dataRegion(String dataRegion) {
        return new Option("DATAREGION", dataRegion);
    }

    public static Option storageProfiles(String storageProfiles) {
        return new Option("STORAGE_PROFILES", storageProfiles);
    }

    public static Option consistencyMode(String consistencyMode) {
        return new Option("CONSISTENCY_MODE", consistencyMode);
    }

    public static Option name(String name) {
        return new Option("NAME", name);
    }

    public static Option tableName(String tableName) {
        return new Option("TABLE_NAME", tableName);
    }

    public static Option schemaName(String tableName) {
        return new Option("SCHEMA_NAME", tableName);
    }

    public static Option zoneName(String zoneName) {
        return new Option("ZONE_NAME", zoneName);
    }

    public static Option indexId(int indexId) {
        return new Option("INDEX_ID", indexId);
    }

    public static Option filter(String filter) {
        return new Option("DATA_NODES_FILTER", filter);
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql(name).sql("=");
        boolean isStringValue = value instanceof String;
        if (isStringValue) {
            String strValue = value.toString();
            char quoteChar = '\'';
            if (!QueryUtils.isQuoted(strValue, quoteChar)) {
                ctx.sql(quoteChar).sql(strValue).sql(quoteChar);
            }
        } else {
            ctx.sql(value.toString());
        }
    }
}
