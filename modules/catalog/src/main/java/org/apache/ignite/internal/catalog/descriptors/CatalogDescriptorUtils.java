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

package org.apache.ignite.internal.catalog.descriptors;

import static java.util.stream.Collectors.toList;

import org.apache.ignite.internal.schema.configuration.ColumnTypeView;
import org.apache.ignite.internal.schema.configuration.ColumnView;
import org.apache.ignite.internal.schema.configuration.TableView;

/**
 * Helper class for working with catalog descriptors.
 */
// TODO: IGNITE-19646 поменять на тикет где будет удалена конфигурация
public class DescriptorUtils {
    /**
     * @param tableConfigView Table configuration.
     */
    public static TableDescriptor of(TableView tableConfigView) {
        TableDescriptor tableDescriptor = new TableDescriptor(
                tableConfigView.id(),
                tableConfigView.name(),
                tableConfigView.columns().stream().map(DescriptorUtils::of).collect(toList())
        )
    }

    private static TableColumnDescriptor of(ColumnView columnConfigView) {
        ColumnTypeView typeConfigView = columnConfigView.type();

        return new TableColumnDescriptor(
                columnConfigView.name(),
                typeConfigView,
                columnConfigView.nullable()
        );
    }
}
