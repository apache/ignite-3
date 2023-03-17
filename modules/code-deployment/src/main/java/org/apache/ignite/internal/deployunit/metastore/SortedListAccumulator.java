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

package org.apache.ignite.internal.deployunit.metastore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.internal.metastorage.Entry;

/**
 * Plain list accumulator. The resulted list will be sorted.
 *
 * @param <T> Result value type.
 */
public class SortedListAccumulator<T extends Comparable<T>> implements Accumulator<List<T>> {
    private final Function<Entry, T> mapper;

    private final List<T> result = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param mapper Value mapper.
     */
    public SortedListAccumulator(Function<Entry, T> mapper) {
        this.mapper = mapper;
    }

    @Override
    public void accumulate(Entry value) {
        T apply = mapper.apply(value);
        if (apply != null) {
            result.add(apply);
        }
    }

    @Override
    public List<T> get() throws AccumulateException {
        Collections.sort(result);
        return result;
    }
}
