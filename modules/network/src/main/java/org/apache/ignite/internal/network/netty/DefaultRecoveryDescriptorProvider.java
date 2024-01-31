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
                .filter(entry -> entry.getKey().launchId().equals(launchId))
                .map(Entry::getValue)
                .collect(toList());
    }

    @Override
    public Collection<RecoveryDescriptor> getAllRecoveryDescriptors() {
        return List.copyOf(recoveryDescriptors.values());
    }
}
