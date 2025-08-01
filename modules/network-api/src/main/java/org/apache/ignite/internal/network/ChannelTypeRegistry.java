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

import java.util.Collection;

/** {@link ChannelType} registry. */
public interface ChannelTypeRegistry {
    /** Returns the {@link ChannelType} by {@link ChannelType#id() ID}. */
    ChannelType get(short id);

    /** Returns an immutable and not empty collection of all {@link ChannelType} unique by {@link ChannelType#id()}. */
    Collection<ChannelType> getAll();

    /** Creates new instance. */
    static ChannelTypeRegistry of(Collection<ChannelType> channelTypes) {
        assert !channelTypes.isEmpty();

        ChannelType[] sortedArray = channelTypes.stream()
                .sorted(comparingInt(ChannelType::id))
                .toArray(ChannelType[]::new);

        if (sortedArray[0].id() == 0 && sortedArray[sortedArray.length - 1].id() == sortedArray.length - 1) {
            return new ArrayChannelTypeRegistry(sortedArray);
        }

        return new MapChannelTypeRegistry(sortedArray);
    }
}
