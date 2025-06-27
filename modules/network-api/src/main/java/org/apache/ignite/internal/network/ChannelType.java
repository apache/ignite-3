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

import org.apache.ignite.internal.tostring.S;

/**
 * Data class with channel information. May be used as channel pointer in {@link MessagingService} for sending messages in exclusive
 * channel.
 *
 * <p>Each new instance must be registered in {@link ChannelTypeRegistry} using {@link ChannelTypeModule}.</p>
 */
public final class ChannelType {
    /** Default channel type. */
    public static final ChannelType DEFAULT = new ChannelType((short) 0, "Default");

    private final short id;

    private final String name;

    /** Constructor. */
    public ChannelType(short id, String name) {
        assert id >= 0 : id;

        this.id = id;
        this.name = name;
    }

    /** Channel ID, must be unique for each implementation. */
    public short id() {
        return id;
    }

    /** Returns channel name. */
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
        return id() == type.id();
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
