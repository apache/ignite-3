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
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/** Represents a storage unit as entry. */
public interface Entry extends Serializable {
    /** Returns the key bytes. */
    byte[] key();

    /** Returns a value. Could be {@code null} for {@link #empty empty} or {@link #tombstone tombstone} entry. */
    byte @Nullable [] value();

    /**
     * Returns the metastorage revision in which the entry was created, {@code 0} for an {@link #empty empty entry}.
     *
     * <p>Revision is increased either by changing one key or the entire batch.</p>
     */
    long revision();

    /**
     * Returns the metastorage update counter in which the entry was created, {@code 0} for an {@link #empty empty entry}.
     *
     * <p>Update counter increases both when one key and each key in the batch are changed under one revision.</p>
     */
    long updateCounter();

    /** Returns {@code true} if entry is empty (never existed or was destroyed by the compaction), otherwise - {@code false}. */
    boolean empty();

    /**
     * Returns {@code true} if entry is tombstone (existed but was removed and has not yet been destroyed by the compaction), otherwise -
     * {@code false}.
     */
    boolean tombstone();

    /**
     * Returns the metastorage operation timestamp in which the entry was created, {@code null} for an {@link #empty empty entry}.
     *
     * <p>Operation timestamp is assigned for each change of the key or the entire batch under one revision.</p>
     */
    @Nullable HybridTimestamp timestamp();
}
