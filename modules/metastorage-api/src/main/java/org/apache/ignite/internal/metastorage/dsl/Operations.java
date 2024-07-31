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

import static org.apache.ignite.internal.util.ArrayUtils.EMPTY_BYTE_BUFFER;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.lang.ByteArray;

/**
 * Class to accumulate some operations to one batch, which can form complete {@link Update} statement together with statement result.
 * Also, this class contains fabric methods which produce operations
 * needed for conditional multi update functionality (invoke) provided by meta storage service.
 *
 * @see Operation
 * @see Update
 */
public final class Operations {
    private static final MetaStorageMessagesFactory MSG_FACTORY = new MetaStorageMessagesFactory();

    /** No-op operation singleton. */
    private static final Operation NO_OP = MSG_FACTORY.operation().type(OperationType.NO_OP).build();

    /** Operations. */
    private final List<Operation> operations;

    /**
     * Constructs new operations batch.
     *
     * @param operations operations.
     */
    private Operations(Operation... operations) {
        this.operations = List.of(operations);
    }

    /**
     * Produce new {@link Update} statement with boolean result.
     *
     * @param result boolean result.
     * @return update statement.
     */
    public Update yield(boolean result) {
        StatementResult statementResult = MSG_FACTORY.statementResult()
                .result(ByteBuffer.allocate(1).put((byte) (result ? 1 : 0)).flip())
                .build();

        return MSG_FACTORY.update()
                .operations(operations)
                .result(statementResult)
                .build();
    }

    /**
     * Produce new {@link Update} statement with integer result.
     *
     * @param result integer result.
     * @return update statement.
     */
    public Update yield(int result) {
        StatementResult statementResult = MSG_FACTORY.statementResult()
                .result(ByteBuffer.allocate(Integer.BYTES).putInt(result).flip())
                .build();

        return MSG_FACTORY.update()
                .operations(operations)
                .result(statementResult)
                .build();
    }

    /**
     * Produce new {@link Update} statement with empty result.
     *
     * @return update statement.
     */
    public Update yield() {
        StatementResult statementResult = MSG_FACTORY.statementResult()
                .result(EMPTY_BYTE_BUFFER)
                .build();

        return MSG_FACTORY.update()
                .operations(operations)
                .result(statementResult)
                .build();
    }

    /**
     * Shortcut to create new batch of operations, for dsl-like API, see {@link Iif}.
     *
     * @param operations operations.
     * @return batch of operations.
     */
    public static Operations ops(Operation... operations) {
        return new Operations(operations);
    }

    /**
     * Creates operation of type <i>remove</i>. This type of operation removes entry.
     *
     * @param key Identifies an entry which operation will be applied to.
     * @return Operation of type <i>remove</i>.
     */
    public static Operation remove(ByteArray key) {
        return MSG_FACTORY.operation()
                .key(ByteBuffer.wrap(key.bytes()))
                .type(OperationType.REMOVE)
                .build();
    }

    /**
     * Creates operation of type <i>put</i>. This type of operation inserts or updates value of entry.
     *
     * @param key Identifies an entry which operation will be applied to.
     * @param value Value.
     * @return Operation of type <i>put</i>.
     */
    public static Operation put(ByteArray key, byte[] value) {
        return put(ByteBuffer.wrap(key.bytes()), ByteBuffer.wrap(value));
    }

    /**
     * Creates operation of type <i>put</i>. This type of operation inserts or updates value of entry.
     *
     * @param key Identifies an entry which operation will be applied to.
     * @param value Value.
     * @return Operation of type <i>put</i>.
     */
    public static Operation put(ByteArray key, ByteBuffer value) {
        return put(ByteBuffer.wrap(key.bytes()), value);
    }

    /**
     * Creates operation of type <i>put</i>. This type of operation inserts or updates value of entry.
     *
     * @param key Identifies an entry which operation will be applied to.
     * @param value Value.
     * @return Operation of type <i>put</i>.
     */
    public static Operation put(ByteBuffer key, ByteBuffer value) {
        return MSG_FACTORY.operation()
                .key(key)
                .value(value)
                .type(OperationType.PUT)
                .build();
    }

    /**
     * Creates operation of type <i>noop</i>. It is a special type of operation which doesn't perform any action.
     *
     * @return Operation of type <i>noop</i>.
     */
    public static Operation noop() {
        return NO_OP;
    }
}
