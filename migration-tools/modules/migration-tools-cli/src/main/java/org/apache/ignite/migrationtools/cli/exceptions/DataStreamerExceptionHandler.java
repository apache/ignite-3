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

package org.apache.ignite.migrationtools.cli.exceptions;

import org.apache.ignite.migrationtools.persistence.mappers.RecordAndTableSchemaMismatchException;
import org.apache.ignite3.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite3.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite3.table.DataStreamerException;

/** DataStreamerExceptionHandler. */
public class DataStreamerExceptionHandler implements ExceptionHandler<DataStreamerException> {
    @Override
    public int handle(ExceptionWriter writer, DataStreamerException e) {
        if (e.getCause() instanceof RecordAndTableSchemaMismatchException) {
            return RecordAndTableSchemaMismatchExceptionHandler.INSTANCE.handle(
                    writer, (RecordAndTableSchemaMismatchException) e.getCause());
        }

        return DefaultMigrateCacheExceptionHandler.INSTANCE.handle(writer, e);
    }

    @Override
    public Class<DataStreamerException> applicableException() {
        return DataStreamerException.class;
    }
}
