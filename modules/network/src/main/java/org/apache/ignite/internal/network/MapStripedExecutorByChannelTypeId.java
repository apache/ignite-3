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

package org.apache.ignite.internal.network;

import static org.apache.ignite.internal.util.CollectionUtils.toIntMapCollector;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.ignite.internal.thread.StripedExecutor;

/** Map-based implementation allows working with gaps in {@link ChannelType#id()}. */
class MapStripedExecutorByChannelTypeId implements StripedExecutorByChannelTypeId {
    private final Int2ObjectMap<StripedExecutor> map;

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26177
    @SuppressWarnings("PMD.UnnecessaryCast")
    MapStripedExecutorByChannelTypeId(CriticalStripedThreadPoolExecutorFactory factory, ChannelType... channelTypes) {
        map = Arrays.stream(channelTypes)
                .collect(toIntMapCollector(
                        channelType -> (int) channelType.id(),
                        factory::create
                ));
    }

    @Override
    public StripedExecutor get(short channelTypeId) {
        StripedExecutor stripedExecutor = map.get(channelTypeId);

        assert stripedExecutor != null : "Channel type is not registered: " + channelTypeId;

        return stripedExecutor;
    }

    @Override
    public Stream<StripedExecutor> stream() {
        return map.values().stream();
    }
}
