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

package org.apache.ignite.internal.catalog;


import java.util.BitSet;

/** Represent result of applying Catalog command. */
public class CatalogApplyResult {
    private final int catalogVersion;
    private final BitSet applied;

    /**
     * Creates an result with a bitset of applied results and version of a catalog when the changes was visible.
     *
     * @param appliedResults Result of applying of commands. Every 1 bit say about applied result, otherwise 0 says that command has
     *      not been applied. Order of bits the same as commands given for execution to Catalog.
     * @param catalogVersion Version of a catalog when the changes was visible.
     */
    public CatalogApplyResult(BitSet appliedResults, int catalogVersion) {
        assert appliedResults != null;

        this.applied = appliedResults;
        this.catalogVersion = catalogVersion;
    }

    /** Returns catalog version since applied result is available. */
    public int getCatalogVersion() {
        return catalogVersion;
    }

    /**
     * Returns has been applied command by given index or not.
     *
     * @param idx Index of command in the result.
     * @return By given index return {@code true} if command has been successfully applied or {@code false} otherwise.
     */
    public boolean isApplied(int idx) {
        return applied.get(idx);
    }
}
