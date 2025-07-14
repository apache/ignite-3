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
import org.apache.ignite3.internal.cli.core.style.component.ErrorUiComponent;

/** RecordAndTableSchemaMismatchExceptionHandler. */
public class RecordAndTableSchemaMismatchExceptionHandler implements ExceptionHandler<RecordAndTableSchemaMismatchException> {
    public static final RecordAndTableSchemaMismatchExceptionHandler INSTANCE = new RecordAndTableSchemaMismatchExceptionHandler();

    @Override
    public int handle(ExceptionWriter writer, RecordAndTableSchemaMismatchException e) {
        StringBuilder msgBuilder = new StringBuilder();
        msgBuilder.append("Mismatch between cache records and the target table definition."
                + "\nAt least one record in the cache was not compatible with the target table definition.");

        if (!e.missingColumnsInRecord().isEmpty()) {
            msgBuilder.append("\nRecord did not have the following fields required by the table: ")
                            .append(String.join(", ", e.missingColumnsInRecord()));
        }

        if (!e.additionalColumnsInRecord().isEmpty()) {
            msgBuilder.append("\nThe following fields were present on the record but not found in the table: ")
                    .append(String.join(", ", e.additionalColumnsInRecord()));
        }

        writer.write(
                ErrorUiComponent.builder()
                        .header(DefaultMigrateCacheExceptionHandler.HEADER)
                        .details(msgBuilder.toString())
                .build()
                .render());

        return 1;
    }

    @Override
    public Class<RecordAndTableSchemaMismatchException> applicableException() {
        return RecordAndTableSchemaMismatchException.class;
    }
}
