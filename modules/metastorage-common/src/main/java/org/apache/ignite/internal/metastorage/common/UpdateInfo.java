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

package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;
import java.util.Collection;
import org.apache.ignite.internal.metastorage.common.command.OperationInfo;

/**
 * Simple operations + result wrapper definition to describe the terminal branch
 * of {@link org.apache.ignite.internal.metastorage.common.command.IfInfo} execution.
 */
public class UpdateInfo implements Serializable {
    /** Operations. */
    private final Collection<OperationInfo> ops;

    /** Result. */
    private final StatementResultInfo result;

    /**
     * Constructs new update definition.
     *
     * @param ops operations
     * @param result result
     */
    public UpdateInfo(Collection<OperationInfo> ops, StatementResultInfo result) {
        this.ops = ops;
        this.result = result;
    }

    /**
     * Retunrs operations.
     *
     * @return operations.
     */
    public Collection<OperationInfo> operations() {
        return ops;
    }

    /**
     * Returns result.
     *
     * @return result.
     */
    public StatementResultInfo result() {
        return result;
    }
}
