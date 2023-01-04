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

package org.apache.ignite.internal.metastorage.command.info;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.command.MetastorageCommandsMessageGroup;
import org.apache.ignite.internal.metastorage.command.MultiInvokeCommand;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Defines if-statement for {@link MultiInvokeCommand}.
 */
@Transferable(MetastorageCommandsMessageGroup.IF_INFO)
public interface IfInfo extends NetworkMessage, Serializable {
    /**
     * Returns boolean condition definition.
     *
     * @return Boolean condition definition.
     */
    ConditionInfo cond();

    /**
     * Returns definition of execution branch, if condition evaluates to true (aka left branch).
     *
     * @return Left execution branch definition.
     */
    StatementInfo andThen();

    /**
     * Returns definition of execution branch, if condition evaluates to false (aka right branch).
     *
     * @return Right execution branch definition.
     */
    StatementInfo orElse();
}
