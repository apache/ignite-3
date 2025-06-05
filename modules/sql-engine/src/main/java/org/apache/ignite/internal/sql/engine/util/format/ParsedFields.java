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

package org.apache.ignite.internal.sql.engine.util.format;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.sql.engine.util.format.DateTimeTemplateField.FieldKind;

/**
 * Parsed fields.
 */
final class ParsedFields {

    private final Map<FieldKind, Object> values = new EnumMap<>(FieldKind.class);

    void add(FieldKind field, Object value) {
        Objects.requireNonNull(field, "field");
        Objects.requireNonNull(value, "value");

        Object prev = this.values.put(field, value);
        if (prev != null) {
            throw new IllegalStateException("Field appeared more than once " + field);
        }
    }

    Map<FieldKind, Object> values() {
        return values;
    }
}
