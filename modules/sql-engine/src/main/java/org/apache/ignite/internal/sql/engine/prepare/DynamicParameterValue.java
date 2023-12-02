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

package org.apache.ignite.internal.sql.engine.prepare;

import org.jetbrains.annotations.Nullable;

/**
 * Dynamic parameter value.
 */
public final class DynamicParameterValue {

    @Nullable
    private final Object value;

    private final boolean hasValue;

    private DynamicParameterValue(@Nullable Object value, boolean hasValue) {
        this.value = value;
        this.hasValue = hasValue;
    }

    /** Creates a dynamic parameter with the given value. */
    public static DynamicParameterValue value(@Nullable Object v) {
        return new DynamicParameterValue(v, true);
    }

    /** Returns a value of this parameter. Should not be called on dynamic parameter with no value. */
    @Nullable
    public Object value() {
        assert hasValue : "Dynamic parameter has no value";
        return value;
    }

    /** Creates a dynamic parameter with no value. */
    public static DynamicParameterValue noValue() {
        return new DynamicParameterValue(null, false);
    }

    /** Returns {@code true} if value is specified. */
    public boolean hasValue() {
        return hasValue;
    }

    /** Creates an array of dynamic parameter values for the given array of values. */
    public static DynamicParameterValue[] fromValues(Object[] values) {
        if (values.length == 0) {
            return new DynamicParameterValue[0];
        } else {
            DynamicParameterValue[] dynamicParams = new DynamicParameterValue[values.length];
            for (int i = 0; i < dynamicParams.length; i++) {
                dynamicParams[i] = value(values[i]);
            }
            return dynamicParams;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        if (hasValue) {
            String str = (value != null ? value.getClass().getName() : "null");
            return "{value: <" + str + ">}";
        } else {
            return "{no-value}";
        }
    }
}
