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

package org.apache.ignite.internal.sql.engine.exec.fsm;

import org.apache.ignite.internal.sql.engine.AsyncSqlCursor;
import org.apache.ignite.internal.sql.engine.SqlQueryType;

/** Handler that adds an additional delay to before publishing the cursor. */
class CursorPublicationPhaseHandler implements ExecutionPhaseHandler {
    static final ExecutionPhaseHandler INSTANCE = new CursorPublicationPhaseHandler();

    private CursorPublicationPhaseHandler() { }

    @Override
    public Result handle(Query query) {
        AsyncSqlCursor<?> cursor = query.cursor;

        assert cursor != null;

        SqlQueryType queryType = cursor.queryType();

        if (queryType == SqlQueryType.QUERY) {
            // Preserve lazy execution for statements that only reads.
            return Result.completed();
        }

        // For other types let's wait for the first page to make sure premature
        // close of the cursor won't cancel an entire operation.
        return Result.proceedAfter(cursor.onFirstPageReady());
    }
}
