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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.Criteria;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.table.criteria.CriteriaException;

/**
 * This utility class provides an ability to map internal SQL exceptions to public CriteriaException.
 */
public class CriteriaExceptionMapperUtil {
    /**
     * This method provides a mapping from internal SQL exception to public ones.
     *
     * <p>The rules of mapping are the following:</p>
     * <ul>
     *     <li>any instance of {@link Error} is returned as is, except {@link AssertionError}
     *     that will always be mapped to {@link CriteriaException} with the {@link Common#INTERNAL_ERR} error code.</li>
     *     <li>any instance of {@link TraceableException} is wrapped into {@link CriteriaException}
     *         with the original {@link TraceableException#traceId() traceUd} and {@link TraceableException#code() code}.</li>
     *     <li>if there are no any mappers that can do a mapping from the given error to a public exception,
     *     then {@link CriteriaException} with the {@link Common#INTERNAL_ERR} error code is returned.</li>
     * </ul>
     *
     * @param origin Exception to be mapped.
     * @return Public exception.
     */
    static Throwable mapToPublicCriteriaException(Throwable origin) {
        Throwable e = mapToPublicException(origin);

        if (e instanceof Error) {
            return e;
        }

        if (e instanceof SqlException) {
            SqlException sqle = (SqlException) e;

            if (sqle.code() == Sql.CURSOR_NO_MORE_PAGES_ERR) {
                return new CriteriaException(Criteria.CURSOR_NO_MORE_PAGES_ERR, e);
            }

            if (sqle.code() == Sql.CURSOR_CLOSED_ERR) {
                return new CriteriaException(Criteria.CURSOR_CLOSED_ERR, e);
            }

            return new CriteriaException(INTERNAL_ERR, e);
        }

        if (e instanceof TraceableException) {
            TraceableException traceable = (TraceableException) e;
            return new CriteriaException(traceable.traceId(), traceable.code(), e.getMessage(), e);
        }

        return new CriteriaException(INTERNAL_ERR, e);
    }

    /**
     * Returns a new CompletableFuture that, when the given {@code origin} future completes exceptionally, maps the origin's exception to a
     * public exception if it is needed.
     *
     * @param origin The future to use to create a new stage.
     * @param <T> Type os result.
     * @return New CompletableFuture.
     */
    public static <T> CompletableFuture<T> convertToPublicFuture(CompletableFuture<T> origin) {
        return origin
                .handle((res, e) -> {
                    if (e != null) {
                        throw new CompletionException(mapToPublicCriteriaException(unwrapCause(e.getCause())));
                    }

                    return res;
                });
    }
}
