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

package org.apache.ignite.internal.cli.decorators;

import com.jakewharton.fliptables.FlipTableConverters;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;
import org.apache.ignite.internal.cli.sql.table.Table;
import org.apache.ignite.internal.cli.util.PlainTableRenderer;
import org.apache.ignite.internal.cli.util.TableTruncator;

/**
 * Implementation of {@link Decorator} for {@link Table}.
 *
 * <p>Uses raw {@code Table} type to match the CLI decorator registry infrastructure.
 * The cast to {@code Table<String>} is safe because all Table instances in the CLI are String-typed.
 */
public class TableDecorator implements Decorator<Table, TerminalOutput> {
    private final boolean plain;
    private final TableTruncator truncator;

    /**
     * Creates a new TableDecorator with truncation disabled.
     *
     * @param plain whether to use plain formatting
     */
    public TableDecorator(boolean plain) {
        this(plain, TruncationConfig.disabled());
    }

    /**
     * Creates a new TableDecorator with truncation support.
     *
     * @param plain whether to use plain formatting
     * @param truncationConfig truncation configuration
     */
    public TableDecorator(boolean plain, TruncationConfig truncationConfig) {
        this.plain = plain;
        this.truncator = new TableTruncator(truncationConfig);
    }

    /** {@inheritDoc} */
    @Override
    public TerminalOutput decorate(Table table) {
        Table<String> truncatedTable = truncator.truncate((Table<String>) table);
        if (plain) {
            return () -> PlainTableRenderer.render(truncatedTable.header(), truncatedTable.content());
        } else {
            return () -> FlipTableConverters.fromObjects(truncatedTable.header(), truncatedTable.content());
        }
    }
}
