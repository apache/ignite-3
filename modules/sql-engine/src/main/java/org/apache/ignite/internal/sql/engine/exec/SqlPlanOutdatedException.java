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

package org.apache.ignite.internal.sql.engine.exec;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.ErrorGroups.Common;

/**
 * Exception occurs when SQL engine detects that the execution plan is outdated after preparation.
 *
 * <p>It is used internally to signal the SQL engine to retry the optimization step
 * using the transaction {@link InternalTransaction#schemaTimestamp() schema timestamp}.
 * This exception should never be passed to the user, and has no special error code.
 */
public class SqlPlanOutdatedException extends IgniteInternalException {
    private static final long serialVersionUID = 1L;

    public SqlPlanOutdatedException() {
        super(Common.INTERNAL_ERR);
    }
}
