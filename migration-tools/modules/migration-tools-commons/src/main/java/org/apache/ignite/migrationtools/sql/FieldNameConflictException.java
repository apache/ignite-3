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

package org.apache.ignite.migrationtools.sql;

import java.util.Set;
import org.apache.ignite.migrationtools.types.InspectedField;

/** Defines a situation when it was not possible to assign a field name due to a conflict with other fields. */
public class FieldNameConflictException extends RuntimeException {

    private FieldNameConflictException(String message) {
        super(message);
    }

    public FieldNameConflictException(InspectedField inspectedField, Set<String> knownFieldNames) {
        super("Duplicated field name for:" + inspectedField + " in " + knownFieldNames);
    }

    public static FieldNameConflictException forSpecificField(String fieldName, String expectedValue, String foundValue) {
        return new FieldNameConflictException(
                String.format("Unexpected type for field: %s - Expected:%s Found:%s", fieldName, expectedValue, foundValue));
    }

    public static FieldNameConflictException forSpecificField(String label, String fieldName) {
        return new FieldNameConflictException(String.format("Unexpected null type for %s field with name: %s", label, fieldName));
    }

    /**
     * Creates an exception for an Unknown conflict type.
     *
     * @param fieldName Field name.
     * @param fieldType Field type.
     * @return The created exception.
     */
    public static FieldNameConflictException forUnknownType(String fieldName, String fieldType) {
        return new FieldNameConflictException(
                String.format("Unknown non-native type for field: %s - %s. Consider adding this type to the classpath.",
                        fieldName, fieldType));
    }
}
