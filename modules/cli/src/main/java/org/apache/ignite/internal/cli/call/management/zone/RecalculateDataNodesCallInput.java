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

package org.apache.ignite.internal.cli.call.management.zone;

import java.util.List;
import org.apache.ignite.internal.cli.commands.zone.datanodes.RecalculateDataNodesMixin;
import org.apache.ignite.internal.cli.core.call.CallInput;

/**
 * Input for {@link RecalculateDataNodesCall}.
 */
public class RecalculateDataNodesCallInput implements CallInput {
    private final List<String> zoneNames;
    private final String clusterUrl;

    private RecalculateDataNodesCallInput(List<String> zoneNames, String clusterUrl) {
        this.zoneNames = zoneNames;
        this.clusterUrl = clusterUrl;
    }

    /**
     * Creates input from mixin and cluster URL.
     *
     * @param mixin Mixin with command options.
     * @param clusterUrl Cluster URL.
     * @return Call input.
     */
    public static RecalculateDataNodesCallInput of(RecalculateDataNodesMixin mixin, String clusterUrl) {
        return new RecalculateDataNodesCallInput(mixin.zoneNames(), clusterUrl);
    }

    /** Returns zone names to recalculate data nodes for. */
    public List<String> zoneNames() {
        return zoneNames;
    }

    /** Returns cluster URL. */
    public String clusterUrl() {
        return clusterUrl;
    }
}
