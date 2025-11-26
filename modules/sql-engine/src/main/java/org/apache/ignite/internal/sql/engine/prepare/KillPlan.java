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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.internal.sql.engine.SqlQueryType;
import org.apache.ignite.internal.sql.engine.exec.kill.KillCommand;
import org.apache.ignite.internal.sql.engine.prepare.partitionawareness.PartitionAwarenessMetadata;
import org.apache.ignite.internal.sql.engine.prepare.pruning.PartitionPruningMetadata;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.ResultSetMetadata;
import org.jetbrains.annotations.Nullable;

/**
 * KILL command execution plan.
 */
public class KillPlan implements QueryPlan {
    /** Kill command metadata holder. */
    private static final ResultSetMetadata KILL_METADATA = new ResultSetMetadataImpl(List.of(
            new ColumnMetadataImpl("APPLIED", ColumnType.BOOLEAN, 1, ColumnMetadata.UNDEFINED_SCALE, false, null)));

    private static final ParameterMetadata EMPTY_PARAMETERS = new ParameterMetadata(Collections.emptyList());

    private final PlanId id;
    private final KillCommand cmd;

    KillPlan(PlanId id, KillCommand cmd) {
        this.id = id;
        this.cmd = cmd;
    }

    public KillCommand command() {
        return cmd;
    }

    /** {@inheritDoc} */
    @Override
    public PlanId id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public SqlQueryType type() {
        return SqlQueryType.KILL;
    }

    /** {@inheritDoc} */
    @Override
    public ResultSetMetadata metadata() {
        return KILL_METADATA;
    }

    /** {@inheritDoc} */
    @Override
    public ParameterMetadata parameterMetadata() {
        return EMPTY_PARAMETERS;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionAwarenessMetadata partitionAwarenessMetadata() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionPruningMetadata partitionPruningMetadata() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public int numSources() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return cmd.toString();
    }
}
