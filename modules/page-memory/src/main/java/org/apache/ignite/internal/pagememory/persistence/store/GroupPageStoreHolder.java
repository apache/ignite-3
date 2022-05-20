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

package org.apache.ignite.internal.pagememory.persistence.store;

import java.util.AbstractList;

/**
 * Holder of the group page stores (index and partitions).
 */
class GroupPageStoreHolder extends AbstractList<PageStore> {
    /** Index page store. */
    final PageStore idxStore;

    /** Partition page stores. */
    final PageStore[] partStores;

    /**
     * Constructor.
     *
     * @param idxStore Index page store.
     * @param partStores Partition page stores.
     */
    public GroupPageStoreHolder(PageStore idxStore, PageStore[] partStores) {
        this.idxStore = idxStore;
        this.partStores = partStores;
    }

    /** {@inheritDoc} */
    @Override
    public PageStore get(int index) {
        return index == partStores.length ? idxStore : partStores[index];
    }

    /** {@inheritDoc} */
    @Override
    public int size() {
        return partStores.length + 1;
    }
}
