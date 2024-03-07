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

package org.apache.ignite.internal.sql.engine.util;

import java.util.function.Consumer;
import org.apache.ignite.internal.sql.engine.QueryProcessor;
import org.apache.ignite.internal.sql.engine.util.QueryChecker.QueryTemplate;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;

/** Interface for {@link QueryChecker} factory. */
public interface QueryCheckerFactory {
    /** Creates query checker instance. */
    QueryChecker create(
            String nodeName,
            QueryProcessor queryProcessor,
            IgniteTransactions transactions,
            @Nullable InternalTransaction tx,
            String query
    );

    /** Creates query checker with custom metadata validator. */
    QueryChecker create(
            String nodeName,
            QueryProcessor queryProcessor,
            IgniteTransactions transactions,
            Consumer<ResultSetMetadata> metadataValidator,
            QueryTemplate queryTemplate
    );
}
