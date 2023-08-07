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

package org.apache.ignite.internal.catalog.descriptors;

import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;

/**
 * Data storage descriptor.
 */
// TODO: IGNITE-19719 Must be storage engine specific
public class CatalogDataStorageDescriptor implements Serializable {
    private static final long serialVersionUID = -5268530663660582126L;
    private final String engine;

    private final String dataRegion;

    /**
     * Constructor.
     *
     * @param engine Storage engine name.
     * @param dataRegion Data region name within the storage engine.
     */
    public CatalogDataStorageDescriptor(String engine, String dataRegion) {
        this.engine = engine;
        this.dataRegion = dataRegion;
    }

    /**
     * Returns the storage engine name.
     */
    public String getEngine() {
        return engine;
    }

    /**
     * Returns the data region name within the storage engine.
     */
    public String getDataRegion() {
        return dataRegion;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
