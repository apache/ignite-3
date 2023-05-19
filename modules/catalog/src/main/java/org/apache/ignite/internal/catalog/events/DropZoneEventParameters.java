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

/**
 * Drop distribution zone event parameters contains an id of dropped distribution zone.
 */
public class DropZoneEventParameters extends CatalogEventParameters {

    private final int zoneId;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     * @param zoneId An id of dropped distribution zone.
     */
    public DropZoneEventParameters(long causalityToken, int zoneId) {
        super(causalityToken);

        this.zoneId = zoneId;
    }

    /** Returns an id of dropped distribution zone. */
    public int zoneId() {
        return zoneId;
    }
}
