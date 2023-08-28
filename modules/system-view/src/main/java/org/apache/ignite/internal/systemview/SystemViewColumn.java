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

package org.apache.ignite.internal.systemview;

import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.lang.ErrorGroups.SysView;
import org.apache.ignite.lang.IgniteException;

/**
 * System view column.
 *
 * @param <T> Row type of a system view.
 * @param <C> Column value type.
 * @see SystemView
 */
public class SystemViewColumn<T, C> {

    private final String name;

    private final Function<T, C> value;

    private final Class<C> type;

    SystemViewColumn(String name, Class<C> type, Function<T, C> value) {
        if (name == null) {
            throw new IgniteException(SysView.VIEW_DEFINITION_ERR, "Column name null can not be null");
        }
        if (type == null) {
            throw new IgniteException(SysView.VIEW_DEFINITION_ERR, "Column type null can not be null");
        }
        if (value == null) {
            throw new IgniteException(SysView.VIEW_DEFINITION_ERR, "Column value null can not be null");
        }

        this.name = name;
        this.type = type;
        this.value = value;
    }

    /**
     * Returns the column name.
     *
     * @return The name of a column.
     */
    public String name() {
        return name;
    }

    /**
     * Returns column value type.
     *
     * @return Column value type.
     */
    public Class<C> type() {
        return type;
    }

    /**
     * Returns a function that reads a value of this column.
     *
     * @return The function that returns a value of this column.
     */
    public Function<T, C> value() {
        return value;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "SystemViewColumn{"
                + "name='" + name + '\''
                + ", type=" + type
                + '}';
    }
}
