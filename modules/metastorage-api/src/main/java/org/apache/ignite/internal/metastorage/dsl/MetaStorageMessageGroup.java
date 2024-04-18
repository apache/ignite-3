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

package org.apache.ignite.internal.metastorage.dsl;

import org.apache.ignite.internal.metastorage.dsl.SimpleCondition.RevisionCondition;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition.ValueCondition;
import org.apache.ignite.internal.metastorage.dsl.Statement.IfStatement;
import org.apache.ignite.internal.metastorage.dsl.Statement.UpdateStatement;
import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message group for Meta Storage messages.
 */
@MessageGroup(groupType = 222, groupName = "MetaStorageMessages")
public interface MetaStorageMessageGroup {
    /** Message type for {@link SimpleCondition}. */
    short SIMPLE_CONDITION = 1;

    /** Message type for {@link RevisionCondition}. */
    short REVISION_CONDITION = 2;

    /** Message type for {@link ValueCondition}. */
    short VALUE_CONDITION = 3;

    /** Message type for {@link CompoundCondition}. */
    short COMPOUND_CONDITION = 4;

    /** Message type for {@link Operation}. */
    short OPERATION = 5;

    /** Message type for {@link StatementResult}. */
    short STATEMENT_RESULT = 6;

    /** Message type for {@link Update}. */
    short UPDATE = 7;

    /** Message type for {@link Iif}. */
    short IF = 8;

    /** Message type for {@link IfStatement}. */
    short IF_STATEMENT = 9;

    /** Message type for {@link UpdateStatement}. */
    short UPDATE_STATEMENT = 10;
}
