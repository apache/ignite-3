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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Helper class to keep customized logic isolated from RelJson. This makes it easier to track upstream changes and rebase this code when
 * upgrading Calcite versions.
 */
class IgniteRelJsonEnumCache {

    private final Map<String, Enum<?>> enums;

    private IgniteRelJsonEnumCache(Map<String, Enum<?>> enums) {
        this.enums = enums;
    }

    <E extends Enum<E>> E get(String name) {
        return (E) requireNonNull(getNullable(name), name);
    }

    @Nullable
    Enum<?> getNullable(String name) {
        return enums.get(name);
    }

    <E extends Enum<E>> E resolveFrom(Object name) {
        assert name instanceof String && enums.containsKey(name);

        return get((String) name);
    }

    static Builder builder() {
        return new Builder();
    }

    static class Builder {

        private final Map<String, Enum<?>> enums = new HashMap<>();

        Builder register(Class<? extends Enum<?>> aclass) {
            String prefix = aclass.getSimpleName() + "#";
            for (Enum<?> enumConstant : aclass.getEnumConstants()) {
                enums.put(prefix + enumConstant.name(), enumConstant);
            }

            return this;
        }

        IgniteRelJsonEnumCache build() {
            return new IgniteRelJsonEnumCache(Map.copyOf(enums));
        }
    }
}
