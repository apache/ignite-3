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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Represents a condition for a meta storage conditional update.
 */
@Transferable(MetaStorageMessageGroup.SIMPLE_CONDITION)
public interface SimpleCondition extends Condition {
    /** Entry key. */
    ByteBuffer key();

    /** Condition type. */
    ConditionType conditionType();

    /**
     * Represents a condition on an entry revision.
     */
    @Transferable(MetaStorageMessageGroup.REVISION_CONDITION)
    interface RevisionCondition extends SimpleCondition {
        /** The revision as the condition argument. */
        long revision();
    }

    /**
     * Represents a condition on an entry value.
     */
    @Transferable(MetaStorageMessageGroup.VALUE_CONDITION)
    interface ValueCondition extends SimpleCondition {
        /** The value as the condition argument. */
        ByteBuffer value();
    }
}
