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

package org.apache.ignite.internal.sql.engine.externalize;

import static org.apache.ignite.internal.util.IgniteUtils.igniteClassLoader;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

final class IgniteRelJsonUtils {

    static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    @Nullable
    static Class<?> classForNameOrNull(String typeName) {
        try {
            return IgniteUtils.forName(typeName, igniteClassLoader());
        } catch (ClassNotFoundException ignored) {
            return null;
        }
    }

    static Class<?> classForName(String typeName) {
        try {
            return IgniteUtils.forName(typeName, igniteClassLoader());
        } catch (ClassNotFoundException ignored) {
            throw new IgniteInternalException(INTERNAL_ERR, "RelJson unable to load type: " + typeName);
        }
    }

}
