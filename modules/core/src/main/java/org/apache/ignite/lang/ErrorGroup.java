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

import static java.util.regex.Pattern.DOTALL;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents a concept of error group. Error group defines a collection of errors that belong to a single semantic component.
 * Each group can be identified by a name and an integer number that both must be unique across all error groups.
 */
public class ErrorGroup {
    /** Additional prefix that is used in a human-readable format of ignite errors. */
    public static final String ERR_PREFIX = "IGN-";

    /** Error message pattern. */
    private static final Pattern EXCEPTION_MESSAGE_PATTERN =
            Pattern.compile("(.*)(IGN)-([A-Z]+)-(\\d+)\\s(TraceId:)([a-f0-9]{8}(?:-[a-f0-9]{4}){4}[a-f0-9]{8})(\\s?)(.*)", DOTALL);

    /** List of all registered error groups. */
    private static final Int2ObjectMap<ErrorGroup> registeredGroups = new Int2ObjectOpenHashMap<>();

    /** Group name. */
    private final String groupName;

    /** Group code. */
    private final short groupCode;

    /** Contains error codes for this error group. */
    private final IntSet codes = new IntOpenHashSet();

    /**
     * Creates a new error group with the specified name and corresponding code.
     *
     * @param groupName Group name.
     * @param groupCode Group code.
     */
    private ErrorGroup(String groupName, short groupCode) {
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
     * Creates a new error group with the given {@code groupName} and {@code groupCode}.
     *
     * @param groupName Group name to be created.
     * @param groupCode Group code to be created.
     * @return New error group.
     * @throws IllegalArgumentException If the specified name or group code already registered.
     *      Also, this exception is thrown if the given {@code groupName} is {@code null} or empty.
     */
    public static synchronized ErrorGroup newGroup(String groupName, short groupCode) {
        if (groupName == null || groupName.isEmpty()) {
            throw new IllegalArgumentException("Group name is null or empty");
        }

        String grpName = groupName.toUpperCase(Locale.ENGLISH);

        if (registeredGroups.containsKey(groupCode)) {
            throw new IllegalArgumentException(
                    "Error group already registered [groupName=" + groupName + ", groupCode=" + groupCode
                            + ", registeredGroup=" + registeredGroups.get(groupCode) + ']');
        }

        for (ErrorGroup group : registeredGroups.values()) {
            if (group.name().equals(groupName)) {
                throw new IllegalArgumentException(
                    "Error group already registered [groupName=" + groupName + ", groupCode=" + groupCode
                            + ", registeredGroup=" + group + ']');
            }
        }

        ErrorGroup newGroup = new ErrorGroup(grpName, groupCode);

        registeredGroups.put(groupCode, newGroup);

        return newGroup;
    }

    /**
     * Returns group code extracted from the given full error code.
     *
     * @param code Full error code.
     * @return Group code.
     */
    public static short extractGroupCode(int code) {
        return (short) (code >>> 16);
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
     * Returns error group identified by the given {@code groupCode}.
     *
     * @param groupCode Group code
     * @return Error Group.
     */
    public static ErrorGroup errorGroupByGroupCode(short groupCode) {
        return registeredGroups.get(groupCode);
    }

    /**
     * Returns error group identified by the given error {@code code}.
     *
     * @param code Full error code
     * @return Error Group.
     */
    public static ErrorGroup errorGroupByCode(int code) {
        return registeredGroups.get(extractGroupCode(code));
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
        return errorMessage(traceId, registeredGroups.get(extractGroupCode(code)).name(), code, message);
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
        return ERR_PREFIX + groupName + '-' + Short.toUnsignedInt(extractErrorCode(code)) + " TraceId:" + traceId
                + ((message != null && !message.isEmpty()) ? ' ' + message : "");
    }

    /**
     * Creates a new error message with predefined prefix.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param cause Cause.
     * @return New error message with predefined prefix.
     */
    public static String errorMessageFromCause(UUID traceId, int code, Throwable cause) {
        return errorMessageFromCause(traceId, registeredGroups.get(extractGroupCode(code)).name(), code, cause);
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

        if (c != null) {
            c = extractCauseMessage(c);
        }

        return errorMessage(traceId, groupName, code, c);
    }

    /**
     * Returns a message extracted from the given {@code errorMessage} if this {@code errorMessage} matches
     * {@link #EXCEPTION_MESSAGE_PATTERN}. If {@code errorMessage} does not match the pattern or {@code null} then returns the original
     * {@code errorMessage}.
     *
     * @param errorMessage Message that is returned by {@link Throwable#getMessage()}
     * @return Extracted message.
     */
    public static @Nullable String extractCauseMessage(String errorMessage) {
        if (errorMessage == null) {
            return null;
        }

        Matcher m = EXCEPTION_MESSAGE_PATTERN.matcher(errorMessage);
        return (m.matches()) ? m.group(8) : errorMessage;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "ErrorGroup [name=" + name() + ", groupCode=" + groupCode() + ']';
    }
}
