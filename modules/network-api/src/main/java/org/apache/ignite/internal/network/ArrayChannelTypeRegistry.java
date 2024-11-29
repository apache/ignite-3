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

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;
import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import java.util.Collection;

/** Array-based implementation allows to increase performance. */
class ArrayChannelTypeRegistry implements ChannelTypeRegistry {
    private final ChannelType[] array;

    ArrayChannelTypeRegistry(ChannelType... channelTypes) {
        assert !nullOrEmpty(channelTypes);

        this.array = channelTypes;
    }

    @Override
    public ChannelType get(short id) {
        assert id >= 0 && id < array.length : "Channel type is not registered: " + id;

        return array[id];
    }

    @Override
    public Collection<ChannelType> getAll() {
        return unmodifiableCollection(asList(array));
    }
}
