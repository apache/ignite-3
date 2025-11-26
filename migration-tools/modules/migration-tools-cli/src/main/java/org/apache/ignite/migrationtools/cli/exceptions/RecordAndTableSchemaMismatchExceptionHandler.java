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
        writer.write(
                ErrorUiComponent.builder()
                        .header(DefaultMigrateCacheExceptionHandler.HEADER)
                        .details(details(e))
                .build()
                .render());

        return 1;
    }

    /**
     * Builds the details output.
     *
     * @param e Exception.
     * @return The output as String.
     */
    public static String details(RecordAndTableSchemaMismatchException e) {
        StringBuilder msgBuilder = new StringBuilder();
        msgBuilder.append("Mismatch between cache records and the target table definition."
                + "\nAt least one record in the cache was not compatible with the target table definition.");

        if (!e.missingColumnsInRecord().isEmpty()) {
            msgBuilder.append("\nRecord did not have the following fields required by the table: ")
                    .append(String.join(", ", e.missingColumnsInRecord()))
                    .append("\nConsider the following solutions:")
                    .append("\n * Manually edit the Ignite 3 table schema to make the missing columns nullable.");
        }

        if (!e.additionalColumnsInRecord().isEmpty()) {
            msgBuilder.append("\nThe following fields were present on the record but not found in the table: ")
                    .append(String.join(", ", e.additionalColumnsInRecord()))
                    .append("\nConsider the following solutions:")
                    .append("\n * Manual Editing: Edit the Ignite 3 table schema manually to add new columns for the additional fields."
                            + " Ensure that the new column types are compatible with the record type.")
                    .append("\n * Ignore Additional Fields: Use the IGNORE_COLUMN migration mode by applying the '--mode IGNORE_COLUMN'"
                            + " option. This approach will skip the migration of those columns, resulting in the loss of their content.")
                    .append("\n * Store as JSON: Use the '--mode PACK_EXTRA' option to store additional fields as JSON in an extra column."
                            + " While Ignite 3 does not natively support unmarshalling the original record from this column, it can be"
                            + " accessed by another application to retrieve the information contained in these additional fields.");
        }
        return msgBuilder.toString();
    }

    @Override
    public Class<RecordAndTableSchemaMismatchException> applicableException() {
        return RecordAndTableSchemaMismatchException.class;
    }
}
