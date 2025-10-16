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

package org.apache.ignite.internal.metastorage.server;

import java.util.List;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/** Notifier of {@link WatchProcessor} about updating metastorage {@link Entry entries}. */
public class UpdateEntriesEvent implements NotifyWatchProcessorEvent {
    private static final IgniteLogger LOG = Loggers.forClass(UpdateEntriesEvent.class);

    @IgniteToStringInclude
    private final List<Entry> updatedEntries;

    @IgniteToStringInclude
    private final HybridTimestamp timestamp;

    /** Constructor. */
    public UpdateEntriesEvent(List<Entry> updatedEntries, HybridTimestamp timestamp) {
        assert !updatedEntries.isEmpty();

        this.updatedEntries = updatedEntries;
        this.timestamp = timestamp;
    }

    @Override
    public HybridTimestamp timestamp() {
        return timestamp;
    }

    @Override
    public void notify(WatchProcessor watchProcessor) {
        watchProcessor.notifyWatches(updatedEntries.get(0).revision(), updatedEntries, timestamp)
                .whenComplete((res, err) -> {
                    if (err != null) {
                        // TODO: https://issues.apache.org/jira/browse/IGNITE-26731
                        LOG.error("Notify watches failed.", err);
                    }
                });
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
