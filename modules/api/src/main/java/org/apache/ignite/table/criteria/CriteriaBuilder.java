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

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a criteria builder. Implementations of this interface are basic building blocks for performing criteria queries.
 */
public class CriteriaBuilder implements Criteria {
    private final List<Criteria> lsCriteria = new ArrayList<>();
    private final String columnName;

    private CriteriaBuilder(String columnName) {
        this.columnName = columnName;
    }

    public static CriteriaBuilder columnName(String columnName) {
        return new CriteriaBuilder(columnName);
    }

    /**
     * Create a predicate for testing the column is equal to a given value.
     *
     * @param value Column value.
     * @return the created <b>equal</b> predicate instance.
     */
    public <R> CriteriaBuilder equal(R value) {
        return addPredicate(Criteria.equal(columnName, value));
    }

    /** {@inheritDoc} */
    @Override
    public <R, C> void accept(CriteriaVisitor<C> v, @Nullable C context) {
        for (var criteria : lsCriteria) {
            criteria.accept(v, context);
        }
    }

    private CriteriaBuilder addPredicate(Criteria criteria) {
        lsCriteria.add(criteria);

        return this;
    }
}
