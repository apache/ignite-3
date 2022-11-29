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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import static org.apache.ignite.lang.ErrorGroups.Sql.DDL_OPTION_ERR;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * DDL option converter.
 */
class DdlOptionConverter<S, T> {
    private final Class<S> type;

    @Nullable private final Consumer<S> validator;

    private final BiConsumer<T, S> setter;

    /**
     * Constructor.
     *
     * @param type DDL option type.
     * @param validator DDL option value validator.
     * @param setter DDL option value setter.
     */
    DdlOptionConverter(
            Class<S> type,
            @Nullable Consumer<S> validator,
            BiConsumer<T, S> setter
    ) {
        this.type = type;
        this.validator = validator;
        this.setter = setter;
    }

    void convert(Object name, SqlLiteral val, String query, T target) {
        S value;

        try {
            value = val.getValueAs(type);
        } catch (AssertionError | ClassCastException e) {
            throw new IgniteException(DDL_OPTION_ERR, String.format(
                    "Unsuspected DDL option type [option=%s, expectedType=%s, query=%s]",
                    name,
                    type.getSimpleName(),
                    query)
            );
        }

        if (validator != null) {
            try {
                validator.accept(value);
            } catch (Throwable e) {
                throw new IgniteException(DDL_OPTION_ERR, String.format(
                        "DDL option validations failed [option=%s, err=%s, query=%s]",
                        name,
                        e.getMessage(),
                        query
                ), e);
            }
        }

        setter.accept(target, value);
    }
}
