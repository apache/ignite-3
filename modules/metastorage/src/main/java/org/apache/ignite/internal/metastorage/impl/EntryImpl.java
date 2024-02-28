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

package org.apache.ignite.internal.metastorage.impl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import org.apache.ignite.internal.metastorage.Entry;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a storage unit as entry with key, value and revision.
 *
 * <p>Where:
 * <ul>
 *     <li>key - an unique entry's key represented by an array of bytes. Keys are comparable in lexicographic manner.</li>
 *     <li>value - a data which is associated with a key and represented as an array of bytes.</li>
 *     <li>revision - a number which denotes a version of whole meta storage.
 *     Each change (which could include multiple entries) increments the revision. </li>
 *     <li>updateCounter - a number which increments on every update in the change under one revision.</li>
 * </ul>
 *
 * <p>Instance of {@link #EntryImpl} could represent:
 * <ul>
 *     <li>A regular entry which stores a particular key, a value and a revision number.</li>
 *     <li>An empty entry which denotes absence of a regular entry in the meta storage for a given key.
 *     A revision is 0 for such kind of entry.</li>
 *     <li>A tombstone entry which denotes that a regular entry for a given key was removed from the storage at some revision.</li>
 * </ul>
 */
public final class EntryImpl implements Entry {
    private static final long serialVersionUID = 3636551347117181271L;

    /** Key. */
    private final byte[] key;

    /** Value. */
    private final byte @Nullable [] val;

    /** Revision. */
    private final long rev;

    /** Update counter. */
    private final long updCntr;

    /**
     * Construct entry with given parameters.
     *
     * @param key Key.
     * @param val Value.
     * @param rev Revision.
     * @param updCntr Update counter.
     */
    public EntryImpl(byte[] key, byte @Nullable [] val, long rev, long updCntr) {
        this.key = key;
        this.val = val;
        this.rev = rev;
        this.updCntr = updCntr;
    }

    @Override
    public byte[] key() {
        return key;
    }

    @Override
    public byte @Nullable [] value() {
        return val;
    }

    @Override
    public long revision() {
        return rev;
    }

    @Override
    public long updateCounter() {
        return updCntr;
    }

    /**
     * Creates an instance of tombstone entry for a given key and a revision.
     *
     * @param key Key bytes. Couldn't be {@code null}.
     * @param rev Revision.
     * @param updCntr Update counter.
     * @return Empty entry.
     */
    public static Entry tombstone(byte[] key, long rev, long updCntr) {
        return new EntryImpl(key, null, rev, updCntr);
    }

    @Override
    public boolean tombstone() {
        return val == null && rev > 0 && updCntr > 0;
    }

    /**
     * Creates an instance of empty entry for a given key.
     *
     * @param key Key bytes. Couldn't be {@code null}.
     * @return Empty entry.
     */
    public static Entry empty(byte[] key) {
        return new EntryImpl(key, null, 0, 0);
    }

    @Override
    public boolean empty() {
        return val == null && rev == 0 && updCntr == 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EntryImpl entry = (EntryImpl) o;

        if (rev != entry.rev) {
            return false;
        }

        if (updCntr != entry.updCntr) {
            return false;
        }

        if (!Arrays.equals(key, entry.key)) {
            return false;
        }

        return Arrays.equals(val, entry.val);
    }

    @Override
    public int hashCode() {
        int res = Arrays.hashCode(key);

        res = 31 * res + Arrays.hashCode(val);

        res = 31 * res + (int) (rev ^ (rev >>> 32));

        res = 31 * res + (int) (updCntr ^ (updCntr >>> 32));

        return res;
    }

    @Override
    public String toString() {
        return "EntryImpl{"
                + "key=" + new String(key, UTF_8)
                + ", val=" + Arrays.toString(val)
                + ", rev=" + rev
                + ", updCntr=" + updCntr
                + '}';
    }
}
