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

package org.apache.ignite.internal.distributionzones.events;

import org.apache.ignite.internal.event.CausalEventParameters;

/**
 * Parameters of {@link HaZoneTopologyUpdateEvent}.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-23640 Remove this params together with the event.
@Deprecated
public class HaZoneTopologyUpdateEventParams extends CausalEventParameters {
    /** Zone id. */
    private final int zoneId;

    /**
     * Constructor.
     *
     * @param zoneId Zone id.
     * @param revision Metastorage revision of the original event, which triggered this local event.
     */
    public HaZoneTopologyUpdateEventParams(int zoneId, long revision) {
        super(revision);

        this.zoneId = zoneId;
    }

    /**
     * Returns zone id.
     *
     * @return Zone id.
     */
    public int zoneId() {
        return zoneId;
    }
}
