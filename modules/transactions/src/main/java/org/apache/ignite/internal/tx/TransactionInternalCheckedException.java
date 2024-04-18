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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ERR_GROUP;
import static org.apache.ignite.lang.ErrorGroups.errorGroupByCode;
import static org.apache.ignite.lang.ErrorGroups.extractGroupCode;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;

/**
 * Base class for checked transaction exceptions.
 */
public class TransactionInternalCheckedException extends IgniteInternalCheckedException {
    /**
     * Creates a new exception with the error code.
     *
     * @param code Full error code.
     */
    public TransactionInternalCheckedException(int code) {
        super(UUID.randomUUID(), code);
        assert extractGroupCode(code) == TX_ERR_GROUP.groupCode() :
                "Error code does not relate to transaction error group [code=" + code + ']';
    }

    /**
     * Creates a new exception with the given trace id and error code.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     */
    public TransactionInternalCheckedException(UUID traceId, int code) {
        super(traceId, code);
        assert extractGroupCode(code) == TX_ERR_GROUP.groupCode() :
                "Error code does not relate to transaction error group [code=" + code + ']';
    }

    /**
     * Creates a new exception with the given error code and detail message.
     *
     * @param code Full error code.
     * @param message Detail message.
     */
    public TransactionInternalCheckedException(int code, String message) {
        super(code, message);
        assert extractGroupCode(code) == TX_ERR_GROUP.groupCode() :
                "Error code does not relate to transaction error group [code=" + code + ']';
    }

    /**
     * Creates a new exception with the given trace id, error code and detail message.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param message Detail message.
     */
    public TransactionInternalCheckedException(UUID traceId, int code, String message) {
        super(traceId, code, message);
        assert extractGroupCode(code) == TX_ERR_GROUP.groupCode() :
                "Error code does not relate to transaction error group "
                        + "[code=" + code + ", errGroup=" + errorGroupByCode(code).name() + ']';
    }

    /**
     * Creates a new exception with the given error code and cause.
     *
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TransactionInternalCheckedException(int code, Throwable cause) {
        super(code, cause);
        assert extractGroupCode(code) == TX_ERR_GROUP.groupCode() :
                "Error code does not relate to transaction error group "
                        + "[code=" + code + ", errGroup=" + errorGroupByCode(code).name() + ']';
    }

    /**
     * Creates a new exception with the given trace id, error code and cause.
     *
     * @param traceId Unique identifier of this exception.
     * @param code Full error code.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TransactionInternalCheckedException(UUID traceId, int code, Throwable cause) {
        super(traceId, code, cause);
        assert extractGroupCode(code) == TX_ERR_GROUP.groupCode() :
                "Error code does not relate to transaction error group "
                        + "[code=" + code + ", errGroup=" + errorGroupByCode(code).name() + ']';
    }

    /**
     * Creates a new exception with the given error code, detail message and cause.
     *
     * @param code Full error code.
     * @param message Detail message.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public TransactionInternalCheckedException(int code, String message, Throwable cause) {
        super(code, message, cause);
        assert extractGroupCode(code) == TX_ERR_GROUP.groupCode() :
                "Error code does not relate to transaction error group "
                        + "[code=" + code + ", errGroup=" + errorGroupByCode(code).name() + ']';
    }
}
