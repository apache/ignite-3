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

import org.apache.ignite.internal.metastorage.command.MetastorageCommandsMessageGroup;
import org.apache.ignite.internal.metastorage.dsl.ConditionType;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Defines condition for InvokeCommand.
 */
@Transferable(MetastorageCommandsMessageGroup.SIMPLE_CONDITION_INFO)
public interface SimpleConditionInfo extends ConditionInfo {
    /**
     * Returns key.
     *
     * @return Key.
     */
    byte[] key();

    /**
     * Returns condition type.
     *
     * @return Condition type.
     */
    int conditionType();

    /**
     * Returns condition type.
     *
     * @return Condition type.
     */
    default ConditionType type() {
        return ConditionType.values()[conditionType()];
    }

    /**
     * Returns value.
     *
     * @return Value.
     */
    byte[] value();

    /**
     * Returns revision.
     *
     * @return Revision.
     */
    long revision();
}
