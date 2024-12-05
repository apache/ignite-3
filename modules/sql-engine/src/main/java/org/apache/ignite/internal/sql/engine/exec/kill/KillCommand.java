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

package org.apache.ignite.internal.sql.engine.exec.kill;

import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.sql.IgniteSqlKill;
import org.apache.ignite.internal.tostring.S;

/**
 * SQL KILL command.
 */
public class KillCommand {
    private final String operationId;
    private final CancellableOperationType type;
    private final boolean noWait;

    /**
     * Creates KILL command.
     */
    public KillCommand(String operationId, CancellableOperationType type, boolean noWait) {
        this.operationId = operationId;
        this.noWait = noWait;
        this.type = type;
    }

    /**
     * Creates KILL command from the AST node.
     *
     * @param sqlKill SQL KILL AST.
     * @return Kill command.
     */
    public static KillCommand fromSqlCall(IgniteSqlKill sqlKill) {
        String operationId = sqlKill.objectId().getValueAs(String.class);

        assert operationId != null;

        CancellableOperationType type = CancellableOperationType.valueOf(sqlKill.objectType().name());

        return new KillCommand(operationId, type, Boolean.TRUE.equals(sqlKill.noWait()));
    }

    public String operationId() {
        return operationId;
    }

    public CancellableOperationType type() {
        return type;
    }

    public boolean noWait() {
        return noWait;
    }

    @Override
    public String toString() {
        return S.toString(KillCommand.class, this);
    }
}
