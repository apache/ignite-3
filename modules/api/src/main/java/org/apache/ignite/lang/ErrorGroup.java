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

package org.apache.ignite.lang;

import static org.apache.ignite.lang.ErrorGroups.errorGroupByCode;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * This class represents a concept of error group. Error group defines a collection of errors that belong to a single semantic component.
 * Each group can be identified by a name and an integer number that both must be unique across all error groups.
 */
public class ErrorGroup {
    /** Group name. */
    private final String groupName;

    /** Group code. */
    private final short groupCode;

    /** Additional prefix that is used in a human-readable format of error messages. */
    private final String errorPrefix;

    /** Contains error codes for this error group. */
    private final Set<Short> codes = new HashSet<>();

    /**
     * Creates a new error group with the specified name and corresponding code.
     *
     * @param errorPrefix Error prefix.
     * @param groupName Group name.
     * @param groupCode Group code.
     */
    ErrorGroup(String errorPrefix, String groupName, short groupCode) {
        this.groupName = groupName;
        this.groupCode = groupCode;
        this.errorPrefix = errorPrefix;
    }

    /**
     * Returns a error prefix of this group.
     *
     * @return Error prefix.
     */
    public String errorPrefix() {
        return errorPrefix;
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
    public short groupCode() {
        return groupCode;
    }

    /**
     * Registers a new error code within this error group.
     *
     * @param errorCode Error code to be registered.
     * @return Full error code which includes group code and specific error code.
     * @throws IllegalArgumentException If the given {@code errorCode} is already registered.
     */
    public int registerErrorCode(short errorCode) {
        if (codes.contains(errorCode)) {
            throw new IllegalArgumentException("Error code already registered [errorCode=" + errorCode + ", group=" + name() + ']');
        }

        codes.add(errorCode);

        return (groupCode() << 16) | (errorCode & 0xFFFF);
    }

    /**
     * Returns error code extracted from the given full error code.
     *
     * @param code Full error code.
     * @return Error code.
     */
    public static short extractErrorCode(int code) {
        return (short) (code & 0xFFFF);
    }

    /**
     * Creates a new error message with predefined prefix.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Original message.
     * @return New error message with predefined prefix.
     */
    public static String errorMessage(UUID traceId, int code, String message) {
        ErrorGroup errorGroup = errorGroupByCode(code);
        return errorMessage(errorGroup.errorPrefix(), traceId, errorGroup.name(), code, message);
    }

    /**
     * Creates a new error message with predefined prefix.
     *
     * @param errorPrefix Prefix for the error.
     * @param traceId Unique identifier of this exception.
     * @param groupName Group name.
     * @param code Full error code.
     * @param message Original message.
     * @return New error message with predefined prefix.
     */
    static String errorMessage(String errorPrefix, UUID traceId, String groupName, int code, String message) {
        return errorPrefix + "-" + groupName + '-' + Short.toUnsignedInt(extractErrorCode(code))
                + ((message != null && !message.isEmpty()) ? ' ' + message : "") + " TraceId:" + traceId.toString().substring(0, 8);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "ErrorGroup [errorPrefix=" + errorPrefix + ", name=" + name() + ", groupCode=" + groupCode() + ']';
    }
}
