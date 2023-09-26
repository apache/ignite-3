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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.util.StringUtils.hexLong;

import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;

/**
 * Replacement removal candidate. Class represents some page from loaded pages table. Usually candidate is found during random {@link
 * LoadedPagesMap} touch.
 */
public class ReplaceCandidate {
    /** Partition generation saved in map, too old value means page may be safely cleared. */
    private int gen;

    @IgniteToStringInclude
    private long relPtr;

    @IgniteToStringInclude
    private FullPageId fullId;

    /**
     * Constructor.
     *
     * @param gen Partition generation.
     * @param relPtr Relative pointer to page.
     * @param fullId Full page ID.
     */
    public ReplaceCandidate(int gen, long relPtr, FullPageId fullId) {
        this.gen = gen;
        this.relPtr = relPtr;
        this.fullId = fullId;
    }

    /**
     * Returns partition generation saved in map, too old value means page may be safely cleared.
     */
    public int generation() {
        return gen;
    }

    /**
     * Returns relative pointer to page.
     */
    public long relativePointer() {
        return relPtr;
    }

    /**
     * Returns index.
     */
    public FullPageId fullId() {
        return fullId;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(ReplaceCandidate.class, this, "relPtr", hexLong(relPtr));
    }
}
