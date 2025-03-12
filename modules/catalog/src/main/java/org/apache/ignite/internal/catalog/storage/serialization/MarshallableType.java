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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.Objects;
import org.jetbrains.annotations.TestOnly;

/**
 * Holds type information of a serialized object. Examples:
 *
 * <p>Type information for a concrete type:
 * <pre>
 *     var typeInfo = MarshallableType.typeOf(MyClass.class, myTypeId, myTypeVersion);
 * </pre>
 *
 * <p>Type information for a field or a collection that stores elements of different types.
 * <pre>
 *     var typeInfo = MarshallableType.builder(BaseClass.class)
 *       .addVariant(MarshallableEntry.VARIANT_1, version1)
 *       .addVariant(MarshallableEntry.VARIANT_2, version2)
 *       .build;
 * </pre>
 *
 * @param <T> Type.
 */
public final class MarshallableType<T> {

    private final Class<T> type;

    private final Int2IntMap versions;

    private MarshallableType(Class<T> type, Int2IntMap versions) {
        Objects.requireNonNull(type, "type");

        if (versions.isEmpty()) {
            throw new IllegalArgumentException("Type information is not provided for type " + type);
        }
        this.type = type;
        this.versions = new Int2IntOpenHashMap(versions);
    }

    /**
     * Returns a version of a serializer of the given type.
     *
     * @param typeId Type id.
     * @return Serializer version.
     */
    public int version(int typeId) {
        int version = versions.get(typeId);
        if (version == 0) {
            String error = format("Unable to find serializer version. Unexpected typeId: {}. Registered: {}", typeId, versions);
            throw new IllegalArgumentException(error);
        }
        return version;
    }

    /**
     * A convenient method for casting the given option to a type of this class.
     *
     * @param elem Object.
     * @return Element casted to this type.
     */
    public T castElement(Object elem) {
        return type.cast(elem);
    }

    /**
     * Type.
     *
     * @return type of this object.
     */
    public Class<T> type() {
        return type;
    }

    /**
     * Single type.
     *
     * @param entryType Entry type.
     * @param type Class.
     * @param version Version.
     *
     * @return Type information.
     */
    public static <T> MarshallableType<T> typeOf(Class<T> entryType, MarshallableEntryType type, int version) {
        return new MarshallableType<>(entryType, Int2IntMaps.singleton(type.id(), version));
    }

    /**
     * Single type.
     *
     * @param entryType Entry type.
     * @param typeId typeId.
     * @param version Version.
     *
     * @return Type information.
     */
    @TestOnly
    public static <T> MarshallableType<T> typeOf(Class<T> entryType, int typeId, int version) {
        return new MarshallableType<>(entryType, Int2IntMaps.singleton(typeId, version));
    }

    /**
     * Type information builder.
     *
     * @param type Class.
     *
     * @return Type information builder
     */
    public static <T> Builder<T> builder(Class<T> type) {
        return new Builder<>(type);
    }

    /**
     * Type information builder.
     *
     * @param <T> Type.
     */
    public static class Builder<T> {
        private final Int2IntMap versions = new Int2IntOpenHashMap();

        private final Class<T> type;

        private Builder(Class<T> type) {
            this.type = type;
        }

        /**
         * Adds a variant.
         *
         * @param type type.
         * @param version serializer version.
         *
         * @return this
         */
        public Builder<T> addVariant(MarshallableEntryType type, int version) {
            versions.put(type.id(), version);
            return this;
        }

        /**
         * Adds a variant.
         *
         * @param typeId type id.
         * @param version serializer version.
         *
         * @return this
         */
        public Builder<T> addVariant(int typeId, int version) {
            versions.put(typeId, version);
            return this;
        }

        /**
         * Creates type info.
         *
         * @return Serialization type info.
         */
        public MarshallableType<T> build() {
            return new MarshallableType<>(type, versions);
        }
    }
}
