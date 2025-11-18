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

package org.apache.ignite.internal.rest.api.cluster.zone.datanodes;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Set;
import org.apache.ignite.internal.tostring.S;

/** Request to recalculate distributed zones' data nodes. */
@Schema(description = "recalculate distributed zones' data nodes.")
public class DataNodesResetRequest {
    @Schema(description = "Names specifying zones to recalculate datanodes for. Case-sensitive, "
            + "if empty then all zones' data nodes will be recalculated.")
    private final Set<String> zoneNames;

    /** Constructor. */
    @JsonCreator
    public DataNodesResetRequest(
            @JsonProperty("zoneNames") Set<String> zoneNames
    ) {
        this.zoneNames = zoneNames == null ? Set.of() : Set.copyOf(zoneNames);
    }

    /** Returns names specifying zones to recalculate dataq nodes. Empty set means "all zones". */
    @JsonGetter("zoneNames")
    public Set<String> zoneNames() {
        return zoneNames;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
