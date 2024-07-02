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
import org.apache.ignite.lang.ErrorGroups.Common;

/**
 * Thrown when detected a query uses an outdated query plan in implicit transaction.
 *
 * <p>Because an implicit transaction starts after query has been planned, there is a chance the schema can be changed in between.
 * This internal exception is not intended to be thrown up to the user, instead, it must be catched internally,
 * and lead the query falling back to the planning phase.
 */
public class ConcurrentSchemaModificationException extends IgniteInternalException {
    public ConcurrentSchemaModificationException() {
        super(Common.INTERNAL_ERR);
    }
}
