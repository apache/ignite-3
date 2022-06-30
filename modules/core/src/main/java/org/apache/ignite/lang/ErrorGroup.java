/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.lang;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * This class represents a concept of error group. Error group defines a collection of errors that belong to a single semantic component.
 * Each group can be identified by a name and an integer number that both must be unique across all error groups.
 */
public class ErrorGroup {
    /** List of all registered error groups. */
    private static final List<ErrorGroup> registeredGroups = new ArrayList<>();

    /** Group name. */
    private final String groupName;

    /** Group code. */
    private final int groupCode;

    /** Contains error codes for this error group. */
    private final Set<Integer> codes = new HashSet<>();

    /**
     * Creates a new error group with the specified name and corresponding code.
     *
     * @param groupName Group name.
     * @param groupCode Group code.
     */
    private ErrorGroup(String groupName, int groupCode) {
        this.groupName = groupName;
        this.groupCode = groupCode;
    }

    /**
     * Returns a name of this group.
     *
     * @return Group name.
     */
    public String name() {
        return groupName;
    }

    /**
     * Returns a code of this group.
     *
     * @return Group code.
     */
    public int code() {
        return groupCode;
    }

    /**
     * Registers a new error code within this error group.
     *
     * @param errorCode Error code to be registered.
     * @return Full error code which includes group code and specific error code.
     * @throws IllegalArgumentException If the given {@code errorCode} is already registered
     *      or {@code errorCode} is greater than 0xFFFF or less than or equal to 0.
     */
    public int registerErrorCode(int errorCode) {
        if (errorCode <= 0 || errorCode > 0xFFFF) {
            throw new IllegalArgumentException("Error code should be greater than 0 and less than or equal to 0xFFFF");
        }

        if (codes.contains(errorCode)) {
            throw new IllegalArgumentException("Error code already registered [errorCode=" + errorCode + ", group=" + name() + ']');
        }

        codes.add(errorCode);

        return (code() << 16) | (errorCode & 0xFFFF);
    }

    /**
     * Checks that the given {@code code} is registered for this error group.
     *
     * @param code Full error code to be tested.
     * @return {@code true} If the given {@code code} is registered for this error group.
     */
    public boolean isRegistered(ErrorGroup group, int code) {
        return group.codes.contains(code);
    }

    @Override
    public String toString() {
        return "ErrorGroup [name=" + name() + ", code=" + code() + ']';
    }

    /**
     * Creates a new error group with the given {@code groupName} and {@code groupCode}.
     *
     * @param groupName Group name to be created.
     * @param groupCode Group code to be created.
     * @return New error group.
     * @throws IllegalArgumentException If the specified name or group code already registered.
     *      or {@code groupCode} is greater than 0xFFFF or less than or equal to 0.
     */
    public static synchronized ErrorGroup newGroup(String groupName, int groupCode) {
        String grpName = groupName.toUpperCase(Locale.ENGLISH);

        Optional<ErrorGroup> grp = registeredGroups.stream().filter(g -> g.name().equals(grpName)).findFirst();
        if (grp.isPresent()) {
            throw new IllegalArgumentException(
                    "Error group already registered [groupName=" + groupName + ", registeredGroup=" + grp.get() + ']');
        }

        grp = registeredGroups.stream().filter(g -> g.code() == groupCode).findFirst();
        if (grp.isPresent()) {
            throw new IllegalArgumentException(
                    "Error group already registered [groupCode=" + groupCode + ", registeredGroup=" + grp.get() + ']');
        }

        ErrorGroup newGrp = new ErrorGroup(grpName, groupCode);

        registeredGroups.add(newGrp);

        return newGrp;
    }

    /**
     * Returns group code extracted from the given full error code.
     *
     * @param code Full error code.
     * @return Group code.
     */
    public static int extractGroupCode(int code) {
        return code >>> 16;
    }

    /**
     * Returns error code extracted from the given full error code.
     *
     * @param code Full error code.
     * @return Error code.
     */
    public static int extractErrorCode(int code) {
        return code & 0xFFFF;
    }

    /**
     * Creates a new error message with predefined prefix.
     *
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param message Original message.
     * @return New error message with predefined prefix.
     */
    public static String errorMessage(UUID traceId, String groupName, int code, String message) {
        return "IGN-" + groupName + '-' + extractErrorCode(code) + " Trace ID:" + traceId + ((message != null) ? ' ' + message : "");
    }

    /**
     * Creates a new error message with predefined prefix.
     *
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param cause Cause.
     * @return New error message with predefined prefix.
     */
    public static String errorMessageFromCause(UUID traceId, String groupName, int code, Throwable cause) {
        String c = (cause != null && cause.getMessage() != null) ? cause.getMessage() : null;

        return (c != null && c.startsWith("IGN-")) ? c :  errorMessage(traceId, groupName, code, c);
    }
}
