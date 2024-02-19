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

package org.apache.ignite.internal.affinity;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.ByteUtils;

/**
 * Class that encapsulates a set of peers and its metadata.
 */
public class Assignments implements Serializable {
    /** Empty assignments. */
    public static final Assignments NO_ASSIGNMENTS = new Assignments(Collections.emptySet());

    /** Set of peers. */
    @IgniteToStringInclude
    private final Set<Assignment> peers;

    /**
     * Constructor.
     *
     * @param peers Set of peers.
     */
    public Assignments(Set<Assignment> peers) {
        this.peers = peers;
    }

    /**
     * Returns a set of peers, represented by this assignments instance.
     */
    public Set<Assignment> peers() {
        return peers;
    }

    /**
     * Serializes the instance into an array of bytes.
     */
    public byte[] toBytes() {
        return ByteUtils.toBytes(this);
    }

    /**
     * Serializes assignments into an array of bytes.
     *
     * @see #toBytes()
     */
    public static byte[] toBytes(Set<Assignment> assignments) {
        return new Assignments(assignments).toBytes();
    }

    /**
     * Deserializes assignments from the array of bytes.
     */
    public static Assignments fromBytes(byte[] bytes) {
        return ByteUtils.fromBytes(bytes);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
