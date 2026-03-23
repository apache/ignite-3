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

import org.apache.calcite.sql.Symbolizable;

/**
 * Enumeration of supported table properties.
 */
public enum IgniteSqlTablePropertyKey implements Symbolizable {
    MIN_STALE_ROWS_COUNT("MIN STALE ROWS"),
    STALE_ROWS_FRACTION("STALE ROWS FRACTION");

    /** Name of the property as it appears in sql grammar. That is, this name may be used to build valid sql string. */
    public final String sqlName;

    IgniteSqlTablePropertyKey(String sqlName) {
        this.sqlName = sqlName;
    }
}
