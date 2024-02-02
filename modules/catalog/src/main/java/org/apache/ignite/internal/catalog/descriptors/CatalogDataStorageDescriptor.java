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

import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.readNullableString;
import static org.apache.ignite.internal.catalog.storage.serialization.CatalogSerializationUtils.writeNullableString;

import java.io.IOException;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Data storage descriptor.
 */
// TODO: IGNITE-19719 Must be storage engine specific
public class CatalogDataStorageDescriptor {
    public static CatalogObjectSerializer<CatalogDataStorageDescriptor> SERIALIZER = new DataStorageDescriptorSerializer();

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
    public String engine() {
        return engine;
    }

    /**
     * Returns the data region name within the storage engine.
     */
    public String dataRegion() {
        return dataRegion;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }

    /**
     * Serializer for {@link CatalogDataStorageDescriptor}.
     */
    private static class DataStorageDescriptorSerializer implements CatalogObjectSerializer<CatalogDataStorageDescriptor> {
        @Override
        public CatalogDataStorageDescriptor readFrom(IgniteDataInput input) throws IOException {
            String engine = input.readUTF();
            String region = readNullableString(input);

            return new CatalogDataStorageDescriptor(engine, region);
        }

        @Override
        public void writeTo(CatalogDataStorageDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeUTF(descriptor.engine());
            writeNullableString(descriptor.dataRegion(), output);
        }
    }
}
