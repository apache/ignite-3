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

package org.apache.ignite.internal.sql;

import static org.apache.ignite.lang.util.IgniteNameUtils.parseIdentifier;

import java.time.ZoneId;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.table.QualifiedName;

/**
 * Common SQL utilities.
 */
public final class SqlCommon {
    /** Normalized name of the default schema. */
    public static final String DEFAULT_SCHEMA_NAME = IgniteNameUtils.parseIdentifier(QualifiedName.DEFAULT_SCHEMA_NAME);

    /** Default page size. */
    public static final int DEFAULT_PAGE_SIZE = 1024;
    /** Default time-zone ID. */
    public static final ZoneId DEFAULT_TIME_ZONE_ID = ZoneId.of("UTC");

    /** Return normalized column name. */
    public static String normalizedColumnName(ColumnMetadata col) {
        return col instanceof ColumnMetadataImpl ? ((ColumnMetadataImpl) col).normalizedName() : parseIdentifier(col.name());
    }
}
