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

package org.apache.ignite.internal.network.netty;

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptor;
import org.apache.ignite.internal.network.recovery.RecoveryDescriptorProvider;
import org.apache.ignite.internal.tostring.S;

/**
 * Default implementation of the {@link RecoveryDescriptorProvider}.
 */
public class DefaultRecoveryDescriptorProvider implements RecoveryDescriptorProvider {
    // TODO: IGNITE-16954 Make this configurable
    private static final int DEFAULT_QUEUE_LIMIT = 10;

    /** Recovery descriptors. */
    private final Map<ChannelKey, RecoveryDescriptor> recoveryDescriptors = new ConcurrentHashMap<>();

    @Override
    public RecoveryDescriptor getRecoveryDescriptor(String consistentId, UUID launchId, short connectionIndex) {
        var key = new ChannelKey(consistentId, launchId, connectionIndex);

        return recoveryDescriptors.computeIfAbsent(key, channelKey -> new RecoveryDescriptor(DEFAULT_QUEUE_LIMIT));
    }

    @Override
    public Collection<RecoveryDescriptor> getRecoveryDescriptorsByLaunchId(UUID launchId) {
        return recoveryDescriptors.entrySet().stream()
                .filter(entry -> entry.getKey().launchId.equals(launchId))
                .map(Entry::getValue)
                .collect(toList());
    }

    @Override
    public Collection<RecoveryDescriptor> getAllRecoveryDescriptors() {
        return List.copyOf(recoveryDescriptors.values());
    }

    /** Channel key. */
    private static class ChannelKey {
        /** Remote node's consistent id. */
        private final String consistentId;

        /** Remote node's launch id. */
        private final UUID launchId;

        /**
         * Connection id. Every connection between this node and a remote node has a unique connection id,
         * but connections with different nodes may have the same ids.
         */
        private final short connectionId;

        private ChannelKey(String consistentId, UUID launchId, short connectionId) {
            this.consistentId = consistentId;
            this.launchId = launchId;
            this.connectionId = connectionId;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ChannelKey that = (ChannelKey) o;

            if (connectionId != that.connectionId) {
                return false;
            }
            if (!consistentId.equals(that.consistentId)) {
                return false;
            }
            return launchId.equals(that.launchId);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            int result = consistentId.hashCode();
            result = 31 * result + launchId.hashCode();
            result = 31 * result + connectionId;
            return result;
        }

        /** {@inheritDoc} */
        @Override
        public String toString() {
            return S.toString(ChannelKey.class, this);
        }
    }
}
