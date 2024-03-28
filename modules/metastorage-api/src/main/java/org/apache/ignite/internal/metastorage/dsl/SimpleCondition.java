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

import java.nio.charset.StandardCharsets;
import org.apache.ignite.internal.network.ToString;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Represents a condition for a meta storage conditional update.
 */
@Transferable(MetaStorageMessageGroup.SIMPLE_CONDITION)
public interface SimpleCondition extends Condition, ToString {
    /** Entry key. */
    byte[] key();

    /** Condition type. */
    int conditionType();

    /**
     * Returns condition type.
     *
     * @return Condition type.
     */
    default ConditionType type() {
        return ConditionType.values()[conditionType()];
    }

    @Override
    default String toStr() {
        return String.format(
                "%s [type=%s, key=%s]",
                getClass().getSimpleName(), type(), new String(key(), StandardCharsets.UTF_8)
        );
    }

    /**
     * Represents a condition on an entry revision.
     */
    @Transferable(MetaStorageMessageGroup.REVISION_CONDITION)
    interface RevisionCondition extends SimpleCondition, ToString {
        /** The revision as the condition argument. */
        long revision();

        @Override
        default String toStr() {
            return String.format(
                    "%s [type=%s, key=%s, revision=%s]",
                    getClass().getSimpleName(), type(), new String(key(), StandardCharsets.UTF_8), revision()
            );
        }
    }

    /**
     * Represents a condition on an entry value.
     */
    @Transferable(MetaStorageMessageGroup.VALUE_CONDITION)
    interface ValueCondition extends SimpleCondition, ToString {
        /** The value as the condition argument. */
        byte[] value();

        @Override
        default String toStr() {
            return String.format(
                    "%s [type=%s, key=%s]",
                    getClass().getSimpleName(), type(), new String(key(), StandardCharsets.UTF_8)
            );
        }
    }
}
