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

package org.apache.ignite.internal.rest.api.recovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Global partition state schema class.
 */
// TODO remove
@Schema(description = "Information about global partition state.")
public class GlobalPartitionStateResponse {
    private final int partitionId;
    private final String zoneName;
    private final int tableId;
    private final String schemaName;
    private final String tableName;
    private final String state;

    /**
     * Constructor.
     */
    @JsonCreator
    public GlobalPartitionStateResponse(
            @JsonProperty("zoneName") String zoneName,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableId") int tableId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("partitionId") int partitionId,
            @JsonProperty("state") String state
    ) {
        this.partitionId = partitionId;
        this.tableId = tableId;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.zoneName = zoneName;
        this.state = state;
    }

    @JsonGetter("partitionId")
    public int partitionId() {
        return partitionId;
    }

    @JsonGetter("tableId")
    public int tableId() {
        return tableId;
    }

    @JsonGetter("schemaName")
    public String schemaName() {
        return schemaName;
    }

    @JsonGetter("tableName")
    public String tableName() {
        return tableName;
    }

    @JsonGetter("zoneName")
    public String zoneName() {
        return zoneName;
    }

    @JsonGetter("state")
    public String state() {
        return state;
    }
}
