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

package org.apache.ignite.internal.table.criteria;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.table.criteria.Criteria;

/**
 * Basic building blocks for performing criteria queries.
 */
public class Criterias {
    /**
     * Creates a predicate that tests whether the column value is equal to the given value.
     *
     * @param columnName Column name.
     * @param element Expression.
     * @return the created <b>equal</b> predicate instance.
     */
    public static Operation columnValue(String columnName, CriteriaElement element) {
        return Operation.create("{0} {1}", List.of(new StaticText(columnName), element));
    }

    /**
     * Creates the negation of the predicate.
     *
     * @param criteria Criteria.
     * @return the created <b>not</b> predicate instance.
     */
    public static <T> Operation not(Operation criteria) {
        return Operation.create("not {0}", List.of(criteria));
    }

    /**
     * Creates the and of the predicates.
     *
     * @param criterias Criterias.
     * @return the created <b>and</b> predicate instance.
     */
    public static <T> Criteria and(Operation... criterias) {
        var template = IntStream.range(0, criterias.length)
                .mapToObj(i -> String.format("{%d}", i))
                .collect(Collectors.joining(") AND (", "(", ")"));

        return Operation.create(template, List.of(criterias));
    }

    /**
     * Creates the or of the predicates.
     *
     * @param criterias Criterias.
     * @return the created <b>or</b> predicate instance.
     */
    public static <T> Criteria or(Operation... criterias) {
        var template = IntStream.range(0, criterias.length)
                .mapToObj(i -> String.format("{%d}", i))
                .collect(Collectors.joining(") OR (", "(", ")"));

        return Operation.create(template, List.of(criterias));
    }
}
