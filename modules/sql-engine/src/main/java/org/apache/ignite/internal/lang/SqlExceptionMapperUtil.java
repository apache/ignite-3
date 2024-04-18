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

package org.apache.ignite.internal.lang;

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import org.apache.ignite.lang.CursorClosedException;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.sql.SqlException;

/**
 * This utility class provides an ability to map Ignite internal exceptions to public SqlException.
 */
public class SqlExceptionMapperUtil {

    /**
     * This method provides a mapping from internal exception to SQL public ones.
     *
     * <p>The rules of mapping are the following:</p>
     * <ul>
     *     <li>any instance of {@link Error} is returned as is, except {@link AssertionError}
     *     that will always be mapped to {@link SqlException} with the {@link Common#INTERNAL_ERR} error code.</li>
     *     <li>any instance of {@link SqlException}, {@link CursorClosedException} is returned as is</li>
     *     <li>any instance of {@link TraceableException} is wrapped into {@link SqlException}
     *         with the original {@link TraceableException#traceId() traceUd} and {@link TraceableException#code() code}.</li>
     *     <li>if there are no any mappers that can do a mapping from the given error to a public exception,
     *     then {@link SqlException} with the {@link Common#INTERNAL_ERR} error code is returned.</li>
     * </ul>
     *
     * @param origin Exception to be mapped.
     * @return Public exception.
     */
    public static Throwable mapToPublicSqlException(Throwable origin) {
        Throwable e = mapToPublicException(origin);
        if (e instanceof Error) {
            return e;
        }
        if (e instanceof SqlException || e instanceof CursorClosedException) {
            return e;
        }

        if (e instanceof TraceableException) {
            TraceableException traceable = (TraceableException) e;
            return new SqlException(traceable.traceId(), traceable.code(), e.getMessage(), e);
        }

        return new SqlException(INTERNAL_ERR, origin);
    }
}
