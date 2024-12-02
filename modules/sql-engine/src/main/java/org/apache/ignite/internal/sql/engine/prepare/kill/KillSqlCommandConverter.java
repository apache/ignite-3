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

package org.apache.ignite.internal.sql.engine.prepare.kill;

import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlKill;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.SqlException;

/**
 * Converter for SQL KILL command.
 */
public class KillSqlCommandConverter {
    /**
     * Converts KILL statement AST.
     *
     * @param kill SQL KILL statement AST.
     * @return Kill command.
     */
    public KillCommand convert(IgniteSqlKill kill) {
        try {
            String objectId = kill.objectId().toValue();

            assert objectId != null;

            CancellableOperationType type = CancellableOperationType.valueOf(kill.objectType().name());

            return new KillCommand(objectId, type, Boolean.TRUE.equals(kill.noWait()));
        } catch (Throwable t) {
            throw new SqlException(Sql.STMT_VALIDATION_ERR, "Failed to convert KILL command.", t);
        }
    }
}
