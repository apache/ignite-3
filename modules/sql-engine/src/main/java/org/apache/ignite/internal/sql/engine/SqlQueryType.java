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

package org.apache.ignite.internal.sql.engine;

import java.util.EnumSet;
import java.util.Set;

/**
 * Possible query types.
 */
public enum SqlQueryType {
    /** Query. */
    QUERY,

    /** DML. */
    DML,

    /** DDL. */
    DDL,

    /** Explain. */
    EXPLAIN,

    /** Transaction control statements such as {@code START TRANSACTION}, {@code COMMIT},  etc. */
    TX_CONTROL,

    ;

    /** A set of statement types that can run only in single statement mode. **/
    public static final Set<SqlQueryType> SINGLE_STMT_TYPES = EnumSet.complementOf(EnumSet.of(TX_CONTROL));

    /** A set of all query types. **/
    public static final Set<SqlQueryType> ALL = Set.of(values());
}
