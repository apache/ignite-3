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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.jetbrains.annotations.Nullable;

/**
 * DDL command option info.
 */
class DdlCommandOptionInfo<S, T> {
    final Class<T> type;

    @Nullable final Consumer<T> validator;

    final BiConsumer<S, T> setter;

    /**
     * Constructor.
     *
     * @param type DDL option type.
     * @param validator DDL option value validator.
     * @param setter DDL option value updater.
     */
    DdlCommandOptionInfo(Class<T> type, @Nullable Consumer<T> validator, BiConsumer<S, T> setter) {
        this.type = type;
        this.validator = validator;
        this.setter = setter;
    }
}
