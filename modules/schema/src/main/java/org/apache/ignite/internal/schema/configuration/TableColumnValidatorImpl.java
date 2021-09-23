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

package org.apache.ignite.internal.schema.configuration;

import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.TableColumnValidator;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.Validator;

/**
 * SchemaTable validator implementation.
 */
public class TableColumnValidatorImpl implements Validator<TableColumnValidator, NamedListView<ColumnView>> {
    /** Static instance. */
    public static final TableColumnValidatorImpl INSTANCE = new TableColumnValidatorImpl();

    /** {@inheritDoc} */
    @Override public void validate(TableColumnValidator annotation, ValidationContext<NamedListView<ColumnView>> ctx) {
        NamedListView<ColumnView> list = ctx.getNewValue();

        for (String key : list.namedListKeys()) {
            ColumnView view = list.get(key);
        }

    }

    /** Private constructor. */
    private TableColumnValidatorImpl() {
    }
}
