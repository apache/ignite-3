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

import org.apache.ignite.internal.event.EventParameters;

/**
 * Parameters of {@link HaZoneTopologyUpdateEvent}.
 */
// TODO: https://issues.apache.org/jira/browse/IGNITE-23640 Remove this params together with the event.
@Deprecated
public class HaZoneTopologyUpdateEventParams implements EventParameters {
    /** Zone id. */
    private final int zoneId;

    /** Long view of{@link org.apache.ignite.internal.hlc.HybridTimestamp}. */
    private final long timestamp;

    /**
     * Constructor.
     *
     * @param zoneId Zone id.
     * @param timestamp Long view of{@link org.apache.ignite.internal.hlc.HybridTimestamp}.
     */
    public HaZoneTopologyUpdateEventParams(int zoneId, long timestamp) {
        this.zoneId = zoneId;
        this.timestamp = timestamp;
    }

    /**
     * Returns zone id.
     *
     * @return Zone id.
     */
    public int zoneId() {
        return zoneId;
    }

    /**
     * Returns long view of {@link org.apache.ignite.internal.hlc.HybridTimestamp}.
     *
     * @return Timestamp.
     */
    public long timestamp() {
        return timestamp;
    }
}
