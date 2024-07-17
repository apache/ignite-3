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

package org.apache.ignite.internal.pagememory.persistence.replacement;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory.Segment;

/**
 * Abstract page replacement policy.
 */
public abstract class PageReplacementPolicy {
    /** Page memory segment. */
    protected final Segment seg;

    /**
     * Constructor.
     *
     * @param seg Page memory segment.
     */
    protected PageReplacementPolicy(Segment seg) {
        this.seg = seg;
    }

    /**
     * Existing page touched.
     *
     * <p>Note: This method can be invoked under segment write lock or segment read lock.
     *
     * @param relPtr Relative pointer to page.
     */
    public void onHit(long relPtr) {
        // No-op.
    }

    /**
     * New page added.
     *
     * <p>Note: This method always invoked under segment write lock.
     *
     * @param relPtr Relative pointer to page.
     */
    public void onMiss(long relPtr) {
        // No-op.
    }

    /**
     * Page removed from the page memory.
     *
     * @param relPtr Relative pointer to page.
     */
    public void onRemove(long relPtr) {
        // No-op.
    }

    /**
     * Finds page to replace.
     *
     * <p>Note: This method always invoked under segment write lock.
     *
     * @return Relative pointer to page.
     */
    public abstract long replace() throws IgniteInternalCheckedException;
}
