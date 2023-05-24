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

package org.apache.ignite.internal.catalog.commands;

import java.io.Serializable;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Definition of value provider to use as default.
 */
@SuppressWarnings("PublicInnerClass")
public class DefaultValue implements Serializable {
    private static final long serialVersionUID = -3056041395340876711L;

    /**
     * Defines value provider as functional provider.
     *
     * @param name Name of the function to invoke to generate the value
     * @return Default value definition.
     */
    public static DefaultValue functionCall(String name) {
        return new FunctionCall(Objects.requireNonNull(name, "name"));
    }

    /**
     * Defines value provider as a constant value provider.
     *
     * @param value A value to use as default.
     * @return Default value definition.
     */
    public static DefaultValue constant(@Nullable Object value) {
        return new ConstantValue(value);
    }

    /** Types of the defaults. */
    public enum Type {
        /** Default is specified as a constant. */
        CONSTANT,

        /** Default is specified as a call to a function. */
        FUNCTION_CALL
    }

    protected final Type type;

    private DefaultValue(Type type) {
        this.type = type;
    }

    /** Returns type of the default value. */
    public Type type() {
        return type;
    }

    /** Defines default value provider as a function call. */
    public static class FunctionCall extends DefaultValue {
        private static final long serialVersionUID = -8166753714497411236L;
        private final String functionName;

        private FunctionCall(String functionName) {
            super(Type.FUNCTION_CALL);
            this.functionName = functionName;
        }

        /** Returns name of the function to use as value generator. */
        public String functionName() {
            return functionName;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            FunctionCall that = (FunctionCall) o;

            return Objects.equals(functionName, that.functionName);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(type, functionName);
        }
    }

    /** Defines default value provider as a constant. */
    public static class ConstantValue extends DefaultValue {
        private static final long serialVersionUID = -5909897953153236118L;
        private final @Nullable Serializable value;

        private ConstantValue(@Nullable Object value) {
            super(Type.CONSTANT);
            this.value = (Serializable) value;
        }

        /** Returns value to use as default. */
        public @Nullable Serializable value() {
            return value;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ConstantValue that = (ConstantValue) o;

            return Objects.equals(value, that.value);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(type, value);
        }
    }
}
