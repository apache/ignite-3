/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.sql;

import java.util.Set;

/** Defines a situation when it was not possible to assign a field name due to a conflict with other fields. */
public class FieldNameConflictException extends RuntimeException {

    private FieldNameConflictException(String message) {
        super(message);
    }

    public FieldNameConflictException(SqlDdlGenerator.InspectedField inspectedField, Set<String> knownFieldNames) {
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
