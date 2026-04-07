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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Functional interface for extracting a value from the current row of a {@link ResultSet}.
 *
 * @param <T> Type of the extracted value.
 */
@FunctionalInterface
public interface RowColumnProjection<T> {
    /** Extracts a value from the current row of {@code rs}. */
    T extract(ResultSet rs) throws SQLException;

    /** Drains result set to list by projecting each record with provided extractor. */
    static <T> List<T> projectRowsColumn(ResultSet rs, RowColumnProjection<T> extractor) throws SQLException {
        List<T> result = new ArrayList<>();

        while (rs.next()) {
            result.add(extractor.extract(rs));
        }

        return result;
    }
}
