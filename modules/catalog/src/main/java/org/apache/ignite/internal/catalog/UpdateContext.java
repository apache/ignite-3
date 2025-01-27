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
 * Context contains two versions of the catalog: the original and the modified.
 * The original (source) version of a catalog can be used by a command from a
 * batch to determine whether certain changes have been made to the catalog
 * during processing of the current batch of commands.
 *
 * @see BulkUpdateProducer
 */
public class UpdateContext {
    /** Source catalog descriptor. */
    private final Catalog originalCatalog;

    /** Catalog descriptor on the basis of which to generate the list of updates. */
    private Catalog updatedCatalog;

    /** Constructor. */
    public UpdateContext(Catalog catalog) {
        this.originalCatalog = catalog;
        this.updatedCatalog = catalog;
    }

    /** Returns catalog descriptor. */
    public Catalog catalog() {
        return updatedCatalog;
    }

    /** Returns source catalog before applying updates from batch. */
    public Catalog baseCatalog() {
        return originalCatalog;
    }

    /** Applies specified action to the catalog. */
    public void updateCatalog(Function<Catalog, Catalog> updater) {
        updatedCatalog = updater.apply(updatedCatalog);
    }
}
