/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.replicator.exception;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Exception utilities.
 */
public class ExceptionUtils {
    /**
     * Restores an exception that has happened in the remote node.
     *
     * @param traceId      Trace id.
     * @param code         Error code.
     * @param errClassName Class name.
     * @param errMsg       Error message.
     * @param stackTrace   Remote error stack trace.
     * @return Restored exception.
     */
    public static IgniteInternalException error(UUID traceId, int code, String errClassName, String errMsg, String stackTrace) {
        IgniteInternalException causeWithStackTrace = stackTrace == null ? null : new IgniteInternalException(traceId, code, stackTrace);

        try {
            Class<?> errCls = Class.forName(errClassName);

            if (IgniteInternalException.class.isAssignableFrom(errCls)) {
                Constructor<?> constructor = errCls.getDeclaredConstructor(UUID.class, int.class, String.class, Throwable.class);

                return (IgniteInternalException) constructor.newInstance(traceId, code, errMsg, causeWithStackTrace);
            }
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException
                | InvocationTargetException ignored) {
            // Ignore: incompatible exception class. Fall back to generic exception.
        }

        return new IgniteInternalException(traceId, code, errClassName + ": " + errMsg, causeWithStackTrace);
    }
}
