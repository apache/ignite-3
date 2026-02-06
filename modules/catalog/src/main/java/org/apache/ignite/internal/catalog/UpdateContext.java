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

import java.util.function.Function;

/**
 * Context contains two instances of the catalog: the base one and the updated one.
 *
 * <p>During batch command processing, changes are generated and applied to
 * the updated instance. The base catalog instance can be used by a command
 * to determine whether certain changes have been made to the catalog during
 * processing of the current batch of commands.
 */
public class UpdateContext {
    /** The base catalog descriptor. */
    private final Catalog baseCatalog;

    /** The updatable catalog descriptor. */
    private Catalog updatableCatalog;

    /** Constructor. */
    public UpdateContext(Catalog catalog) {
        this.baseCatalog = catalog;
        this.updatableCatalog = catalog;
    }

    /**
     * Returns the catalog descriptor on the basis of which the command generates the list of updates.
     *
     * <p>In the case of batch processing, this catalog instance contains the updates made by previous
     * commands in the batch.
     *
     * @return Catalog descriptor.
     */
    public Catalog catalog() {
        return updatableCatalog;
    }

    /** Returns base catalog as it was before any updates from the batch were applied. */
    public Catalog baseCatalog() {
        return baseCatalog;
    }

    /** Applies specified action to the catalog. */
    public void updateCatalog(Function<Catalog, Catalog> updater) {
        updatableCatalog = updater.apply(updatableCatalog);
    }
}
