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

package org.apache.ignite.network;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Data class with channel information.
 *      May be used as channel pointer in {@link MessagingService} for sending messages in exclusive channel.
 */
public final class ChannelInfo {
    static {
        Map<Short, ChannelInfo> tmpChannels = new ConcurrentHashMap<>(IgniteUtils.capacity(Short.MAX_VALUE));
        tmpChannels.put((short) 0, new ChannelInfo((short) 0, "Default"));

        channels = tmpChannels;
    }

    private static final Map<Short, ChannelInfo> channels;

    private static final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Channel identifier.
     */
    private final short id;

    /**
     * Channel name.
     */
    private final String name;

    private ChannelInfo(short id, String name) {
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
        if (!(obj instanceof ChannelInfo)) {
            return false;
        }
        ChannelInfo type = (ChannelInfo) obj;
        return Objects.equals(id(), type.id());
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Returns default channel info.
     *
     * @return Default channel info.
     */
    public static ChannelInfo defaultChannel() {
        lock.readLock().lock();
        try {
            return channels.get((short) 0);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Try to register channel with provided identifier. If identifier already used
     *    by another channel will throw {@link ChannelTypeAlreadyExist}.
     *
     * @param id Channel identifier.
     * @param name Channel name.
     * @return Register channel.
     * @throws ChannelTypeAlreadyExist In case when channel identifier already used.
     */
    public static ChannelInfo register(short id, String name) throws ChannelTypeAlreadyExist {
        lock.writeLock().lock();
        try {
            if (channels.get(id) == null) {
                ChannelInfo result = new ChannelInfo(id, name);
                channels.put(id, result);
                return result;
            }

            throw new ChannelTypeAlreadyExist(id, name);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns channel with provided identifier or
     *      {@code null} if channel with {@param id} doesn't registered yet.
     *
     * @param id Channel identifier.
     * @return Channel with provided identifier or {@code null} if channel with {@param id} doesn't registered yet.
     */
    public static ChannelInfo getChannel(short id) {
        lock.readLock().lock();
        try {
            return channels.get(id);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Generate and return channel with free identifier or {@code null} if all identifiers already used.
     *
     * @param name Channel name.
     * @return Channel with free identifier or {@code null} if all identifiers already used.
     */
    public static @Nullable ChannelInfo generate(String name) {
        lock.writeLock().lock();
        try {
            for (short i = 0; i < Short.MAX_VALUE; i++) {
                if (!channels.containsKey(i)) {
                    ChannelInfo result = new ChannelInfo(i, name);
                    channels.put(i, result);
                    return result;
                }
            }
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
