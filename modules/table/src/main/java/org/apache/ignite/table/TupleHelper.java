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

package org.apache.ignite.table;

import org.apache.ignite.lang.util.IgniteNameUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class to provide direct access to internals of {@link TupleImpl}.
 */
public class TupleHelper {
    /**
     * Returns value from a given tuple which corresponds to a specified column.
     * 
     * <p>Given name is expected to be normalized, thus it's up to caller to invoke 
     * {@link IgniteNameUtils#parseSimpleName(String)} prior passing the name to this method.
     *
     * @param tuple A tuple to get value from.
     * @param normalizedName A column name that is considered to be normalized 
     *      (e.g result of {@link IgniteNameUtils#parseSimpleName(String)}).
     * @param defaultValue A value to return in case tuple doesn't have column with given name.
     * @param <T> A type of the returned value.
     * @return A value from a tuple.
     */
    public static <T> @Nullable T valueOrDefault(Tuple tuple, String normalizedName, @Nullable T defaultValue) {
        if (tuple instanceof TupleImpl) {
            return ((TupleImpl) tuple).valueOrDefaultSkipNormalization(normalizedName, defaultValue);
        }

        return tuple.valueOrDefault(IgniteNameUtils.quote(normalizedName), defaultValue);
    }
}
