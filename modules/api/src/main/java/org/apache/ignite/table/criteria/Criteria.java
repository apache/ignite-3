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

package org.apache.ignite.table.criteria;

import java.util.List;

/**
 * Represents a predicate. Implementations of this interface are basic building blocks for performing criteria queries.
 */
public interface Criteria extends Expression {
    /**
     * Create a predicate for testing the column is equal to a given value.
     *
     * @param columnName Column name.
     * @param value Column value.
     * @return the created <b>equal</b> predicate instance.
     */
    static <T> Criteria equal(String columnName, T value) {
        return Operation.create("{0} = {1}", List.of(new StaticText(columnName), new Constant<>(value)));
    }

    /**
     * Create a predicate for testing the column is equal to a given value.
     *
     * @param criteria Column name.
     * @return the created <b>not</b> predicate instance.
     */
    static <T> Criteria not(Criteria criteria) {
        return Operation.create("not {0}", List.of(criteria));
    }
}
