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

package org.apache.ignite.jdbc.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.junit.jupiter.api.function.Executable;

/**
 * Test utils for JDBC.
 */
public class JdbcTestUtils {
    /**
     * <em>Assert</em> that execution of the supplied {@code executable} throws
     * an {@code T} and return the exception.
     *
     * @param expectedType Expected exception type.
     * @param executable Supplier to execute and check thrown exception.
     * @return Thrown the {@link SQLException}.
     */
    public static <T extends SQLException> T assertThrowsSqlException(Class<T> expectedType, Executable executable) {
        return assertThrows(expectedType, executable);

    }

    /**
     * <em>Assert</em> that execution of the supplied {@code executable} throws
     * an {@link java.sql.SQLException} and return the exception.
     *
     * @param executable Supplier to execute and check thrown exception.
     * @return Thrown the {@link SQLException}.
     */
    public static SQLException assertThrowsSqlException(Executable executable) {
        return assertThrowsSqlException(SQLException.class, executable);
    }

    /**
     * <em>Assert</em> that execution of the supplied {@code executable} throws
     * an {@link SQLException} with expected message.
     *
     * @param expectedMessage Expected error message of {@link SQLException}.
     * @param executable Supplier to execute and check thrown exception.
     * @return Thrown the {@link SQLException}.
     */
    public static SQLException assertThrowsSqlException(String expectedMessage, Executable executable) {
        SQLException ex = assertThrowsSqlException(executable);

        String msg = ex.getMessage();

        assertNotNull(msg, "Error message was null, but expected '" + expectedMessage + "'.");
        assertTrue(msg.contains(expectedMessage), "Error message '" + ex.getMessage() + "' doesn't contain '" + expectedMessage + "'.");

        return ex;
    }

    /**
     * <em>Assert</em> that execution of the supplied {@code executable} throws
     * an {@code T} with expected error message.
     *
     * @param expectedType Expected exception type.
     * @param expectedMessage Expected error message of {@link SQLException}.
     * @param executable Supplier to execute and check thrown exception.
     * @return Thrown the {@link SQLException}.
     */
    public static <T extends SQLException> T assertThrowsSqlException(
            Class<T> expectedType,
            String expectedMessage,
            Executable executable) {
        T ex = assertThrowsSqlException(expectedType, executable);

        String msg = ex.getMessage();

        assertNotNull(msg, "Error message was null, but expected '" + expectedMessage + "'.");
        assertTrue(msg.contains(expectedMessage), "Error message '" + ex.getMessage() + "' doesn't contain '" + expectedMessage + "'.");

        return ex;
    }
}
