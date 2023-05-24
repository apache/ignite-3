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

package org.apache.ignite.internal.catalog.commands.altercolumn;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.AbstractTableCommandParams;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;

/**
 * ALTER TABLE ... ALTER COLUMN statement.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class AlterColumnParams extends AbstractTableCommandParams {
    private String columnName;

    private List<ColumnChangeAction> changeActions = new ArrayList<>(1);

    /** Gets column name. */
    public String columnName() {
        return columnName;
    }

    /** Gets change action for the column descriptor. */
    public ColumnChangeAction action() {
        return (desc) -> {
            TableColumnDescriptor changedDescriptor = desc;
            boolean noop = true;

            for (ColumnChangeAction action : changeActions) {
                TableColumnDescriptor tmp = changedDescriptor;
                changedDescriptor = action.apply(changedDescriptor);

                if (changedDescriptor == null) {
                    changedDescriptor = tmp;
                } else {
                    noop = false;
                }
            }

            return noop ? null : changedDescriptor;
        };
    }

    public static AlterColumnParams.Builder builder() {
        return new AlterColumnParams.Builder();
    }

    /**
     * Parameters builder.
     */
    public static class Builder extends AbstractBuilder<AlterColumnParams, Builder> {
        private Builder() {
            super(new AlterColumnParams());
        }

        /** Sets list of column change actions. */
        public Builder changeActions(List<ColumnChangeAction> changes) {
            params.changeActions = changes.stream().sorted(Comparator.comparing(ColumnChangeAction::priority)).collect(Collectors.toList());

            return this;
        }

        /** Sets column name. */
        public Builder columnName(String name) {
            params.columnName = name;

            return this;
        }
    }
}
