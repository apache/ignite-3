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

import org.apache.ignite.internal.tostring.S;

/**
 * Describes deletion of a zone.
 */
public class DropZoneEntry implements UpdateEntry {
    private static final long serialVersionUID = 7727583734058987315L;

    private final int zoneId;

    /**
     * Constructs the object.
     *
     * @param zoneId An id of a zone to drop.
     */
    public DropZoneEntry(int zoneId) {
        this.zoneId = zoneId;
    }

    /** Returns an id of a zone to drop. */
    public int zoneId() {
        return zoneId;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }
}
