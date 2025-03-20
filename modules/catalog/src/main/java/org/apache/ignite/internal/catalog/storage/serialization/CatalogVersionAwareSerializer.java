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

package org.apache.ignite.internal.catalog.storage.serialization;

import java.io.IOException;

/**
 * Decorator for {@link CatalogObjectSerializer} that adds a method to get the serializer version.
 *
 * @see CatalogObjectSerializer
 */
public class CatalogVersionAwareSerializer<T extends MarshallableEntry> implements CatalogObjectSerializer<T> {
    private final CatalogObjectSerializer<T> delegate;

    private final short version;

    CatalogVersionAwareSerializer(CatalogObjectSerializer<T> delegate, short version) {
        this.delegate = delegate;
        this.version = version;
    }

    @Override
    public T readFrom(CatalogObjectDataInput input)throws IOException {
        return delegate.readFrom(input);
    }

    @Override
    public void writeTo(T value, CatalogObjectDataOutput output) throws IOException {
        delegate.writeTo(value, output);
    }

    public short version() {
        return version;
    }

    public CatalogObjectSerializer<T> delegate() {
        return delegate;
    }
}
