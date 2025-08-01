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

package org.apache.ignite.internal.lang;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.lang.ErrorGroup;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

class ErrorGroupTest {
    @Test
    public void testErrorCodeInitialization() throws Exception {
        ErrorGroups.initialize();

        // Check that all error codes are initialized.
        int errGroupsCount = ErrorGroups.class.getDeclaredClasses().length;
        for (int groupCode = 1; groupCode <= errGroupsCount; groupCode++) {
            ErrorGroup errGroup = ErrorGroups.errorGroupByCode(groupCode << 16);
            assertThat("Error group is not initialized for code: " + groupCode, errGroup, notNullValue());
        }
    }

    @Test
    void extractsCauseMessageFromIgniteExceptionMessage() {
        String igniteExceptionMessage = "IGN-CMN-65535 I'm the reason TraceId:24103638";

        checkExtractCauseMessage(igniteExceptionMessage, "I'm the reason");
    }

    @Test
    void extractsEmptyCauseMessageFromIgniteExceptionMessage() {
        // Given message without the reason of the error
        String igniteExceptionMessage = "IGN-CMN-65535 TraceId:24103638";

        checkExtractCauseMessage(igniteExceptionMessage, "");
    }

    @Test
    void createsErrorMessage() {
        // Given
        UUID traceId = UUID.fromString("24103638-d079-4a19-a8f6-ca9c23662908");
        int code = Common.INTERNAL_ERR;
        String reason = "I'm the reason";

        // When
        String errorMessage = ErrorGroup.errorMessage(traceId, code, reason);

        // Then
        assertThat(errorMessage, equalTo("IGN-CMN-65535 I'm the reason TraceId:24103638"));
    }

    @Test
    void doesNotDuplicateErrorCodeAndTraceId() {
        // Given
        UUID traceId = UUID.fromString("24103638-d079-4a19-a8f6-ca9c23662908");
        int code = Common.INTERNAL_ERR;
        IgniteInternalException cause = new IgniteInternalException(traceId, code, "I'm the\n reason\n");
        IgniteInternalException origin = new IgniteInternalException(traceId, code, cause);

        // When
        String errorMessage = origin.getMessage();
        String detailedMessage = origin.toString();

        // Then error code and traceId are not duplicated
        assertThat(errorMessage, equalTo("I'm the\n reason\n"));
        assertThat(detailedMessage, equalTo(IgniteInternalException.class.getName()
                + ": IGN-CMN-65535 I'm the\n reason\n TraceId:24103638"));
    }

    @SuppressWarnings({"rawtypes", "OptionalGetWithoutIsPresent"})
    @Test
    void groupIdsAreUnique() throws IllegalAccessException {
        Map<Short, ErrorGroup> errGroups = new HashMap<>();

        for (Class cls : ErrorGroups.class.getDeclaredClasses()) {
            var errGroupField = Arrays.stream(cls.getFields()).filter(f -> f.getName().endsWith("_ERR_GROUP")).findFirst().get();
            var errGroup = (ErrorGroup) errGroupField.get(null);

            var existing = errGroups.putIfAbsent(errGroup.groupCode(), errGroup);

            if (existing != null) {
                fail("Duplicate error group id: " + errGroup.groupCode() + " (" + existing.name() + ", " + errGroup.name() + ")");
            }
        }
    }

    @Test
    public void testExtractCauseMessageForNewErrorGroups() {
        ErrorGroups.initialize();

        ErrorGroups.registerGroup("ERR1", "FIRST", (short) 2000);
        ErrorGroups.registerGroup("ERR2", "SECOND", (short) 2001);

        String exceptionMessage = "IGN-CMN-65535 I'm the reason TraceId:24103638";
        checkExtractCauseMessage(exceptionMessage, "I'm the reason");

        exceptionMessage = "ERR1-FIRST-2000 Second reason TraceId:b8e8b3db";
        checkExtractCauseMessage(exceptionMessage, "Second reason");

        String exceptionMessageWithIncorrectGroupName = "ERR3-SECOND-2001 Unknown reason TraceId:184ab36f";

        checkExtractCauseMessage(exceptionMessageWithIncorrectGroupName, exceptionMessageWithIncorrectGroupName);

    }

    private static void checkExtractCauseMessage(String exceptionMessage, String operand) {
        // When
        String extractedMessage = ErrorGroups.extractCauseMessage(exceptionMessage);
        // Then
        assertThat(extractedMessage, equalTo(operand));
    }

    @Test
    public void testRegisterGroupWithNewErrorPrefix() {
        ErrorGroups.initialize();

        ErrorGroups.registerGroup("CY", "CYPRUS", (short) 1000);
        ErrorGroups.registerGroup("CY", "CYPRUS_SECOND", (short) 1001);
        ErrorGroups.registerGroup("USA", "USA", (short) 1002);
        ErrorGroups.registerGroup("USA", "USA_SECOND", (short) 1003);
        ErrorGroups.registerGroup("CY", "CYPRUS_THIRD", (short) 1004);
        ErrorGroups.registerGroup("USA", "USA_THIRD", (short) 1005);

        checkErrorGroup(1000, "CY", "CYPRUS");
        checkErrorGroup(1001, "CY", "CYPRUS_SECOND");
        checkErrorGroup(1002, "USA", "USA");
        checkErrorGroup(1003, "USA", "USA_SECOND");
        checkErrorGroup(1004, "CY", "CYPRUS_THIRD");
        checkErrorGroup(1005, "USA", "USA_THIRD");

        checkErrorGroup(1, "IGN", Common.COMMON_ERR_GROUP.name());
    }

    private static void checkErrorGroup(int errorCode, String expectedErrorPrefix, String expectedGroupName) {
        ErrorGroup errorGroup = ErrorGroups.errorGroupByGroupCode((short) errorCode);

        assertEquals(expectedErrorPrefix, errorGroup.errorPrefix());
        assertEquals(expectedGroupName, errorGroup.name());
        assertThat(errorGroup.toString(), Matchers.startsWith("ErrorGroup [errorPrefix=" + expectedErrorPrefix));
    }
}
