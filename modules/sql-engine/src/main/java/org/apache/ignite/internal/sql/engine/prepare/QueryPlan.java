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

package org.apache.ignite.internal.sql.engine.prepare;

import java.util.EnumSet;
import java.util.Set;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * QueryPlan interface.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public interface QueryPlan {
    /** Query type. */
    enum Type {
        QUERY, DML, DDL, EXPLAIN
    }

    /**
     * All plan types.
     */
    Set<Type> TOP_LEVEL_TYPES = EnumSet.allOf(Type.class);

    /**
     * Get query type, or {@code null} if this is a fragment.
     */
    @Nullable Type type();

    /**
     * Get fields metadata.
     */
    ResultSetMetadata metadata();

    /**
     * Clones this plan.
     */
    QueryPlan copy();
}
