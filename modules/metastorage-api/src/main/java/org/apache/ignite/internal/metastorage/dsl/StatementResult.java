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

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Simple result of statement execution, backed by byte[] array.
 * Provides some shortcut methods to represent the values of some primitive types.
 */
@Transferable(MetaStorageMessageGroup.STATEMENT_RESULT)
public interface StatementResult extends NetworkMessage, Serializable {
    /** Result data. */
    ByteBuffer result();

    /**
     * Returns result value as a boolean.
     *
     * @return boolean result.
     * @throws IllegalStateException if boolean conversion is not possible, or can have ambiguous behaviour.
     */
    default boolean getAsBoolean() {
        if (result().capacity() != 1) {
            throw new IllegalStateException("Result is too big and can't be interpreted as boolean");
        }

        byte value = result().rewind().get();

        if ((value | 1) != 1) {
            throw new IllegalStateException("Result is ambiguous and can't be interpreted as boolean");
        }

        return value != 0;
    }

    /**
     * Returns result as an int.
     *
     * @return int result.
     * @throws IllegalStateException if int conversion is not possible, or can have ambiguous behaviour.
     */
    default int getAsInt() {
        if (result().capacity() != 4) {
            throw new IllegalStateException("Result can't be interpreted as int");
        }

        return result().rewind().getInt();
    }
}
