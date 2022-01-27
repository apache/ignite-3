/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.extension;

import org.apache.ignite.internal.sql.engine.extension.SqlExtension.ExternalCatalog;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;

/**
 * Listener used to notify {@link SqlSchemaManager} about any changes in the external catalogs.
 */
public interface CatalogUpdateListener {
    /**
     * Notify the {@link SqlSchemaManager} that provided catalog has been updated.
     *
     * @param catalog Catalog to notify the {@link SqlSchemaManager} about.
     */
    void onCatalogUpdated(ExternalCatalog catalog);
}
