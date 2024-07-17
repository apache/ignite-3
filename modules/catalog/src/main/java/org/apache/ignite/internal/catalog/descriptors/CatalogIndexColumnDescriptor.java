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

import java.io.IOException;
import java.util.Objects;
import org.apache.ignite.internal.catalog.storage.serialization.CatalogObjectSerializer;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Indexed column descriptor.
 */
public class CatalogIndexColumnDescriptor {
    public static final CatalogObjectSerializer<CatalogIndexColumnDescriptor> SERIALIZER = new IndexColumnDescriptorSerializer();

    private final String name;

    private final CatalogColumnCollation collation;

    public CatalogIndexColumnDescriptor(String name, CatalogColumnCollation collation) {
        this.name = name;
        this.collation = Objects.requireNonNull(collation, "collation");
    }

    public String name() {
        return name;
    }

    public CatalogColumnCollation collation() {
        return collation;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(this);
    }

    private static class IndexColumnDescriptorSerializer implements CatalogObjectSerializer<CatalogIndexColumnDescriptor> {
        @Override
        public CatalogIndexColumnDescriptor readFrom(IgniteDataInput input) throws IOException {
            String name = input.readUTF();
            CatalogColumnCollation collation = CatalogColumnCollation.unpack(input.readByte());

            return new CatalogIndexColumnDescriptor(name, collation);
        }

        @Override
        public void writeTo(CatalogIndexColumnDescriptor descriptor, IgniteDataOutput output) throws IOException {
            output.writeUTF(descriptor.name());
            output.writeByte(CatalogColumnCollation.pack(descriptor.collation()));
        }
    }
}
