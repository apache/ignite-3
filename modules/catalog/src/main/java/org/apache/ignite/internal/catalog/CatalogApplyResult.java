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


/* Represent result of applying Catalog command. */
public class CatalogApplyResult {
    private final int catalogVersion;
    private final boolean applied;

    /* Creates an applied result with a version of a catalog since the change became visible. */
    static CatalogApplyResult applied(int catalogVersion) {
        return new CatalogApplyResult(catalogVersion, true);
    }

    /* Creates an unapplied result with a version of a catalog when the change was visible. */
    static CatalogApplyResult notApplied(int catalogVersion) {
        return new CatalogApplyResult(catalogVersion, false);
    }

    private CatalogApplyResult(int catalogVersion, boolean applied) {
        this.catalogVersion = catalogVersion;
        this.applied = applied;
    }

    /**
     * @return catalog version since applied result is available.
     */
    public int getCatalogVersion() {
        return catalogVersion;
    }

    /**
     * @return {@code true} if command has been successfully applied or {@code false} otherwise.
     */
    public boolean isApplied() {
        return applied;
    }
}
