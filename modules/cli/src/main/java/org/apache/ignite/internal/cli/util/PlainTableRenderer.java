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

package org.apache.ignite.internal.cli.util;

import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class for the plain rendering of table structures.
 */
public class PlainTableRenderer {
    /** Column delimiter for plain rendering. */
    private static final String COLUMN_DELIMITER = "\t";
    /** Row delimiter for plain rendering. */
    private static final String LINE_SEPARATOR = System.lineSeparator();

    /**
     * Render {@code String[]} and {@code Object[][]} as a table using plain formatting.
     *
     * @param hdr header of table.
     * @param content header of table.
     * @return Plain interpretation.
     */
    public static String render(String[] hdr, Object[][] content) {
        StringJoiner sj = new StringJoiner(LINE_SEPARATOR);
        sj.add(tableRowToString(hdr));
        for (Object[] row : content) {
            sj.add(tableRowToString(row));
        }
        return sj.toString();
    }

    /**
     * Render {@code Object[]} using plain formatting.
     *
     * @param row row of table.
     * @return Plain interpretation of row.
     */
    private static String tableRowToString(Object[] row) {
        return Stream.of(row).map(String::valueOf).collect(Collectors.joining(COLUMN_DELIMITER));
    }
}
