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

package org.apache.ignite.internal.metastorage;

import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;

/**
 * Represent an update event for particular key and entry.
 */
public final class EntryEvent implements Serializable {
    /** Old (previous) entry. */
    private final Entry oldEntry;

    /** New (updated) entry. */
    private final Entry newEntry;

    /**
     * Constructs an event with given old and new entries.
     *
     * @param oldEntry Old entry.
     * @param newEntry New entry.
     */
    public EntryEvent(Entry oldEntry, Entry newEntry) {
        this.oldEntry = oldEntry;
        this.newEntry = newEntry;
    }

    /**
     * Returns old entry.
     *
     * @return Old entry.
     */
    public Entry oldEntry() {
        return oldEntry;
    }

    /**
     * Returns new entry.
     *
     * @return New entry.
     */
    public Entry newEntry() {
        return newEntry;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EntryEvent that = (EntryEvent) o;

        if (!oldEntry.equals(that.oldEntry)) {
            return false;
        }

        return newEntry.equals(that.newEntry);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int res = oldEntry.hashCode();

        res = 31 * res + newEntry.hashCode();

        return res;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
