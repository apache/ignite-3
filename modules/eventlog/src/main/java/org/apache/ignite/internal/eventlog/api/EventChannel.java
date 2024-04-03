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

package org.apache.ignite.internal.eventlog.api;

import java.util.Set;

/**
 * Event channel that groups events by type and sends these events into sinks that are piped into the channel.
 */
public interface EventChannel {
    /**
     * Returns the set of event types that this channel can handle.
     */
    Set<String> types();

    /**
     * Logs the event into the channel. If the event type is not supported by the channel, the exception is thrown.
     *
     * @param event Event to log.
     */
    void log(Event event);
}
