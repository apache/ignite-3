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

package org.apache.ignite.internal.catalog.events;

import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;

/**
 * Create zone event parameters contains a distribution zone descriptor for newly created distribution zone.
 */
public class CreateZoneEventParameters extends CatalogEventParameters {

    private final CatalogZoneDescriptor zoneDescriptor;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @param zoneDescriptor Newly created distribution zone descriptor.
     */
    public CreateZoneEventParameters(long causalityToken, int catalogVersion, CatalogZoneDescriptor zoneDescriptor) {
        super(causalityToken, catalogVersion);

        this.zoneDescriptor = zoneDescriptor;
    }

    /**
     * Gets distribution zone descriptor for newly created distribution zone.
     */
    public CatalogZoneDescriptor zoneDescriptor() {
        return zoneDescriptor;
    }
}
