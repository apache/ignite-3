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
import org.apache.ignite.internal.tostring.S;

/**
 * Describes update of the object id generator.
 */
public class ObjectIdGenUpdateEntry implements UpdateEntry {
    private static final long serialVersionUID = -6550888305785861504L;

    private final int delta;

    /**
     * Constructs the object.
     *
     * @param delta A delta by which to correct the id generator.
     */
    public ObjectIdGenUpdateEntry(int delta) {
        this.delta = delta;
    }

    /** Returns delta by which to correct the id generator. */
    public int delta() {
        return delta;
    }

    @Override
    public Catalog applyUpdate(Catalog catalog, long causalityToken) {
        return new Catalog(
                catalog.version(),
                catalog.time(),
                catalog.objectIdGenState() + delta,
                catalog.zones(),
                catalog.schemas()
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
