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

package org.apache.ignite.internal.schema;

import java.util.Map;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Interface describing value provider to generate column default.
 */
public interface DefaultValueProvider {
    /**
     * Provider that returns {@code null} every time.
     */
    DefaultValueProvider NULL_PROVIDER = new ConstantValueProvider(null);

    /**
     * Type of the value provider.
     */
    enum Type {
        CONSTANT((byte) 0), FUNCTIONAL((byte) 1);

        private final byte id;

        private static final Map<Byte, Type> byId = Map.of(
                CONSTANT.id(), CONSTANT,
                FUNCTIONAL.id(), FUNCTIONAL
        );

        /** Resolves type by it's id. */
        public static @Nullable Type byId(byte id) {
            return byId.get(id);
        }

        Type(byte id) {
            this.id = id;
        }

        /** Returns unique id of the type. */
        public byte id() {
            return id;
        }
    }

    /**
     * Creates value provider from given generator.
     *
     * @param gen Generator to wrap.
     * @return Value provider.
     */
    static DefaultValueProvider forValueGenerator(DefaultValueGenerator gen) {
        return new FunctionalValueProvider(Objects.requireNonNull(gen));
    }

    /**
     * Creates provider from given constant.
     *
     * @param val Value to wrap.
     * @return Value provider.
     */
    static DefaultValueProvider constantProvider(@Nullable Object val) {
        if (val == null) {
            return NULL_PROVIDER;
        }

        return new ConstantValueProvider(val);
    }

    /** Returns type of the given value provider. */
    Type type();

    /** Returns value to use as default. */
    Object get();

    /**
     * Value provider that returns the same value.
     */
    class ConstantValueProvider implements DefaultValueProvider {
        private final @Nullable Object value;

        private ConstantValueProvider(@Nullable Object value) {
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override
        public Type type() {
            return Type.CONSTANT;
        }

        /** {@inheritDoc} */
        @Override
        public Object get() {
            return value;
        }
    }

    /**
     * Value provider that use a given generator as value source.
     */
    class FunctionalValueProvider implements DefaultValueProvider {
        private final DefaultValueGenerator gen;

        private FunctionalValueProvider(DefaultValueGenerator gen) {
            this.gen = gen;
        }

        /** {@inheritDoc} */
        @Override
        public Type type() {
            return Type.FUNCTIONAL;
        }

        public String name() {
            return gen.name();
        }

        /** {@inheritDoc} */
        @Override
        public Object get() {
            return gen.next();
        }
    }
}
