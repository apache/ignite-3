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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.util.ExceptionUtils.copyExceptionWithCause;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.ignite.internal.client.tx.ClientTransactionKilledException;
import org.apache.ignite.internal.lang.IgniteExceptionMapper;
import org.apache.ignite.internal.lang.IgniteExceptionMappersProvider;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteException;

/** Client Module Exception mapper. */
@AutoService(IgniteExceptionMappersProvider.class)
public class ClientExceptionMapperProvider implements IgniteExceptionMappersProvider {
    private static final String RETRIABLE_TX_MESSAGE = "Retriable transaction exception";

    @Override
    public Collection<IgniteExceptionMapper<?, ?>> mappers() {
        return List.of(
                mapException(
                        ClientRetriableTransactionException.class,
                        err -> new ClientRetriableTransactionException(err.code(), RETRIABLE_TX_MESSAGE, null)
                ),
                mapException(
                        ClientTransactionKilledException.class,
                        err -> new ClientTransactionKilledException(err.traceId(), err.code(), RETRIABLE_TX_MESSAGE, err.txId(), null)
                )
        );
    }

    private static <T extends IgniteInternalException> IgniteExceptionMapper<T, IgniteException> mapException(
            Class<T> errType,
            UnaryOperator<T> copyFunc
    ) {
        return IgniteExceptionMapper.unchecked(errType, err -> {
            Throwable cause = err.getCause();
            assert cause.getCause() == null : "Cause of client RetriableTransactionExceptions should have no causes.";
            // Retriable copy is actually a RetriableTransactionException. May not be included in the future to present leaking internals.
            Throwable retriableCopy = copyFunc.apply(err);
            return copyExceptionWithCause(cause.getClass(), err.traceId(), err.code(), err.getMessage(), retriableCopy);
        });
    }
}
