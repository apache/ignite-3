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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.eventlog.api.Event;
import org.apache.ignite.internal.eventlog.api.EventChannel;
import org.apache.ignite.internal.eventlog.api.Sink;

class EventChannelImpl implements EventChannel {
    private final Set<Sink> sinks;

    private final Set<String> types;

    EventChannelImpl(Set<String> types, Set<Sink> sinks) {
        this.types = new HashSet<>(types);
        this.sinks = new HashSet<>(sinks);
    }

    @Override
    public Set<String> types() {
        return types;
    }

    @Override
    public void log(Event event) {
        sinks.forEach(s -> s.write(event));
    }
}
