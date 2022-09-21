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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Simple class descriptor containing simple and fully qualified class names as well as the list of class fields.
 */
class ClassDescriptor {
    /** Class simple name. */
    private final String sqn;

    /** Class FQN. */
    private final String fqn;

    /** Class field descriptors. */
    private final ArrayList<FieldDescriptor> fields = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param cls Class.
     */
    ClassDescriptor(Class<?> cls) {
        assert cls != null;

        fqn = cls.getName();
        sqn = cls.getSimpleName();
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

    /**
     * Returns simple class name.
     *
     * @return Simple class name.
     */
    String getSimpleClassName() {
        return sqn;
    }

    /**
     * Returns fully qualified class name.
     *
     * @return Fully qualified class name.
     */
    String getFullyQualifiedClassName() {
        return fqn;
    }

    /**
     * Returns list of fields.
     *
     * @return List of fields.
     */
    List<FieldDescriptor> getFields() {
        return fields;
    }
}