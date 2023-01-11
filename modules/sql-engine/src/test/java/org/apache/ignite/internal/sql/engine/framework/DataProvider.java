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

package org.apache.ignite.internal.sql.engine.framework;

import java.util.Collection;

/**
 * Producer of the rows to use with {@link TestTable} in execution-related scenarios.
 *
 * <p>A data provider is supposed to be created for table on per-node basis. It's up
 * to developer to keep data provider in sync with the schema of the table this data provider relates to.
 *
 * @param <T> A type of the produced elements.
 * @see TestTable
 */
@FunctionalInterface
public interface DataProvider<T> extends Iterable<T> {
    /**
     * Creates data provider from given collection.
     *
     * @param collection Collection to use as source of data.
     * @param <T> A type of the produced elements.
     * @return A data provider instance backed by given collection.
     */
    static <T> DataProvider<T> fromCollection(Collection<T> collection) {
        return collection::iterator;
    }
}
