/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.vault.common;

import java.util.Comparator;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.Nullable;

/**
 * Watch for vault entries.
 * Could be specified by range of keys.
 * If value of key in range is changed, then corresponding listener will be triggered.
 */
public class VaultWatch {
    /** Comparator for {@code ByteArray} values. */
    private static final Comparator<ByteArray> CMP = ByteArray::compare;

    /** Start key of range (inclusive) */
    @Nullable
    private ByteArray startKey;

    /** End key of range (inclusive) */
    @Nullable
    private ByteArray endKey;

    /** Listener for vault's values updates. */
    private VaultListener listener;

    /**
     * @param listener Listener.
     */
    public VaultWatch(VaultListener listener) {
        this.listener = listener;
    }

    /**
     * Start key of range (inclusive).
     * If value of key in range is changed, then corresponding listener will be triggered.
     *
     * @param startKey Start key represented as {@code ByteArray}.
     */
    public void startKey(ByteArray startKey) {
        this.startKey = startKey;
    }

    /**
     * Start key of range (inclusive).
     * If value of key in range is changed, then corresponding listener will be triggered.
     *
     * @param endKey End key represented as {@code ByteArray}.
     */
    public void endKey(ByteArray endKey) {
        this.endKey = endKey;
    }

    /**
     * Notifies specified listener if {@code val} of key in range was changed.
     *
     * @param val Vault entry.
     */
    public void notify(VaultEntry val) {
        if (startKey != null && CMP.compare(val.key(), startKey) < 0)
            return;

        if (endKey != null && CMP.compare(val.key(), endKey) > 0)
            return;

        listener.onEntryChanged(val);
    }
}
