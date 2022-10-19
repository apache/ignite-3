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

package org.apache.ignite.internal.metastorage.common.command;

import org.apache.ignite.network.annotations.Transferable;

/**
 * Defines compound multi-key condition for {@link MultiInvokeCommand}.
 */
@Transferable(MetastorageCommandsMessageGroup.COMPOUND_CONDITION_INFO)
public interface CompoundConditionInfo extends ConditionInfo {
    /**
     * Returns left condition definition.
     *
     * @return left condition definition.
     */
    ConditionInfo leftConditionInfo();

    /**
     * Returns right condition definition.
     *
     * @return right condition definition.
     */
    ConditionInfo rightConditionInfo();

    /**
     * Returns definition for type of compound condition.
     *
     * @return definition for type of compound condition.
     */
    int conditionType();

    /**
     * Returns definition for type of compound condition.
     *
     * @return definition for type of compound condition.
     */
    default CompoundConditionType type() {
        return CompoundConditionType.values()[conditionType()];
    }
}
