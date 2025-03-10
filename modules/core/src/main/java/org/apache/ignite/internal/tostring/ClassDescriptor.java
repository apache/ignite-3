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

package org.apache.ignite.internal.tostring;

import static org.apache.ignite.internal.tostring.ToStringUtils.createStringifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Simple class descriptor containing simple and fully qualified class names as well as the list of class fields.
 */
class ClassDescriptor {
    /** Class name. */
    private final String name;

    /** Class stringifier, {@code null} if absent. */
    private final @Nullable Stringifier<?> stringifier;

    /** Class field descriptors. */
    private final ArrayList<FieldDescriptor> fields = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param cls Class.
     */
    ClassDescriptor(Class<?> cls) {
        IgniteStringifier igniteStringifier = cls.getAnnotation(IgniteStringifier.class);

        if (igniteStringifier == null) {
            name = cls.getSimpleName();
            stringifier = null;
        } else {
            name = "".equals(igniteStringifier.name()) ? cls.getSimpleName() : igniteStringifier.name();
            stringifier = createStringifier(igniteStringifier.value());
        }
    }

    /**
     * Adds field descriptor.
     *
     * @param field Field descriptor to be added.
     */
    void addField(FieldDescriptor field) {
        assert field != null;

        fields.add(field);
    }

    /**
     * Sort fields.
     */
    void sortFields() {
        fields.trimToSize();

        fields.sort(Comparator.comparingInt(FieldDescriptor::getOrder));
    }

    /** Returns simple class name. */
    String getName() {
        return name;
    }

    /** Returns list of fields. */
    List<FieldDescriptor> getFields() {
        return fields;
    }

    /** Returns class stringifier, {@code null} id absent. */
    @Nullable Stringifier<?> getStringifier() {
        return stringifier;
    }
}
