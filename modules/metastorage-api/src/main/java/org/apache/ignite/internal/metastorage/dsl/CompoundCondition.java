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

import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Compound condition, which consists two other simple/compound conditions.
 */
@Transferable(MetaStorageMessageGroup.COMPOUND_CONDITION)
public interface CompoundCondition extends Condition {
    /**
     * Returns left condition.
     *
     * @return left condition.
     */
    Condition leftCondition();

    /**
     * Returns right condition.
     *
     * @return right condition.
     */
    Condition rightCondition();

    /**
     * Returns compound condition type.
     *
     * @return compound condition type.
     */
    int compoundConditionType();

    /**
     * Returns compound condition type.
     */
    default CompoundConditionType type() {
        return CompoundConditionType.values()[compoundConditionType()];
    }
}
