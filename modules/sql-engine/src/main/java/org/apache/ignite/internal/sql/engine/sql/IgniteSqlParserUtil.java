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

package org.apache.ignite.internal.sql.engine.sql;

import java.math.BigDecimal;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.ignite.internal.generated.query.calcite.sql.IgniteSqlParserImpl;
import org.apache.ignite.internal.sql.engine.util.IgniteResource;

/**
 * Utility functions used by {@link IgniteSqlParserImpl ignite sql parser}.
 */
public class IgniteSqlParserUtil {

    private IgniteSqlParserUtil() {

    }

    /** Parses decimal from a decimal literal value (a string in single quotes). */
    public static BigDecimal parseDecimal(String value, SqlParserPos pos) {
        assert value.startsWith("'") && value.endsWith("'") : "input must be quoted: " + value;

        try {
            return new BigDecimal(value.substring(1, value.length() - 1));
        } catch (NumberFormatException ignore) {
            throw SqlUtil.newContextException(pos, IgniteResource.INSTANCE.decimalLiteralInvalid());
        }
    }
}
