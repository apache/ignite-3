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

import static java.util.Comparator.comparingInt;

import java.util.stream.Stream;
import org.apache.ignite.internal.thread.StripedExecutor;

/** Collection of {@link StripedExecutor} that can be got by {@link ChannelType#id()}. */
interface StripedExecutorByChannelTypeId {
    StripedExecutor get(short channelTypeId);

    Stream<StripedExecutor> stream();

    /** Create new instance. */
    static StripedExecutorByChannelTypeId of(ChannelTypeRegistry channelTypeRegistry, CriticalStripedThreadPoolExecutorFactory factory) {
        ChannelType[] sortedArray = channelTypeRegistry.getAll().stream()
                .sorted(comparingInt(ChannelType::id))
                .toArray(ChannelType[]::new);

        if (sortedArray[0].id() == 0 && sortedArray[sortedArray.length - 1].id() == sortedArray.length - 1) {
            return new ArrayStripedExecutorByChannelTypeId(factory, sortedArray);
        }

        return new MapStripedExecutorByChannelTypeId(factory, sortedArray);
    }
}
