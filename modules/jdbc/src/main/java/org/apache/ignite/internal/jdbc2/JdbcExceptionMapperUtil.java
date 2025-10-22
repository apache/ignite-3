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

package org.apache.ignite.internal.jdbc2;

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.sql.SQLException;
import org.apache.ignite.internal.lang.IgniteExceptionMapperUtil;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteException;

/**
 * Maps an exception to a {@link SQLException}.
 */
final class JdbcExceptionMapperUtil {

    private JdbcExceptionMapperUtil() {

    }

    static SQLException mapToJdbcException(String message, Exception e) {
        return new SQLException(message, IgniteExceptionMapperUtil.mapToPublicException(unwrapCause(e)));
    }

    static SQLException mapToJdbcException(Exception e) {
        Throwable cause = IgniteExceptionMapperUtil.mapToPublicException(unwrapCause(e));
        String message = cause.getMessage();

        if (cause instanceof IgniteException) {
            IgniteException ie = (IgniteException) cause;
            if (ie.code() == Sql.TX_CONTROL_INSIDE_EXTERNAL_TX_ERR) {
                message = "Transaction control statements are not supported when autocommit mode is disabled.";
            }
        }

        return new SQLException(message, cause);
    }
}
