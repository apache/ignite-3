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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Data class with channel information.
 *      May be used as channel pointer in {@link MessagingService} for sending messages in exclusive channel.
 */
public final class ChannelType {
    static {
        Map<Short, ChannelType> tmpChannels = new ConcurrentHashMap<>(IgniteUtils.capacity(Short.MAX_VALUE));
        ChannelType defaultChannel = new ChannelType((short) 0, "Default");
        tmpChannels.put((short) 0, defaultChannel);

        DEFAULT = defaultChannel;
        channels = tmpChannels;
    }

    public static final ChannelType DEFAULT;

    private static final Map<Short, ChannelType> channels;

    /**
     * Channel identifier.
     */
    private final short id;

    /**
     * Channel name.
     */
    private final String name;

    private ChannelType(short id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * Channel identifier, must be unique for each implementation.
     *
     * @return Channel identifier.
     */
    public short id() {
        return id;
    }

    /**
     * Returns channel name.
     *
     * @return Channel name.
     */
    public String name() {
        return name;
    }

    @Override
    public int hashCode() {
        return id();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ChannelType)) {
            return false;
        }
        ChannelType type = (ChannelType) obj;
        return Objects.equals(id(), type.id());
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Try to register channel with provided identifier. If identifier already used
     *    by another channel will throw {@link ChannelTypeAlreadyExist}.
     *
     * @param id Channel identifier. Must be positive.
     * @param name Channel name.
     * @return Register channel or existed one.
     * @throws ChannelTypeAlreadyExist In case when channel identifier already used with another name.
     */
    public static ChannelType register(short id, String name) {
        if (id < 0) {
            throw new IllegalArgumentException("Negative identifier is not supported.");
        }
        ChannelType newChannel = new ChannelType(id, name);
        ChannelType channelType = channels.putIfAbsent(id, newChannel);
        if (channelType != null) {
            throw new ChannelTypeAlreadyExist(id, name);
        }
        return newChannel;
    }

    /**
     * Returns channel with provided identifier or
     *      {@code null} if channel with id doesn't registered yet.
     *
     * @param id Channel identifier.
     * @return Channel with provided identifier or {@code null} if channel with id doesn't registered yet.
     */
    public static ChannelType getChannel(short id) {
        return channels.get(id);
    }
}
