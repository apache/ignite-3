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

package org.apache.ignite.internal.catalog.storage;

import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.storage.serialization.MarshallableEntry;

/**
 * Interface describing a particular change within the {@link VersionedUpdate group}.
 */
public interface UpdateEntry extends MarshallableEntry {
    /**
     * Applies own change to the catalog.
     *
     * @param catalog Current catalog.
     * @param causalityToken Token that is associated with the corresponding update being applied.
     * @return New catalog.
     */
    Catalog applyUpdate(Catalog catalog, long causalityToken);
}
