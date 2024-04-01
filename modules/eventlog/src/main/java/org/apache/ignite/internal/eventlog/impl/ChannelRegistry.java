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

package org.apache.ignite.internal.eventlog.impl;

import java.util.Set;
import org.apache.ignite.internal.eventlog.api.EventChannel;

/**
 * Channel registry. The only way to send an event into channel is to get the channel from this registry.
 * The channel can not be cached for a long time because it can be removed from the registry due to configuration changes.
 */
interface ChannelRegistry {
    /**
     * Get channel by name.
     *
     * @param name Channel name.
     * @return Channel instance.
     */
    EventChannel getByName(String name);

    /**
     * Get all channels that can handle the given event type.
     *
     * @param igniteEventType Ignite event type.
     * @return Set of channels.
     */
    Set<EventChannel> findAllChannelsByEventType(String igniteEventType);
}
