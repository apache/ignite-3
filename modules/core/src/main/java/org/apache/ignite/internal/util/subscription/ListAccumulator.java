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

package org.apache.ignite.internal.util.subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Plain list accumulator.
 *
 * @param <T> Stream elements value type.
 * @param <R> Result value type.
 */
public class ListAccumulator<T, R> implements Accumulator<T, List<R>> {
    private final Function<T, R> mapper;

    private final List<R> result = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param mapper Value mapper.
     */
    public ListAccumulator(Function<T, R> mapper) {
        this.mapper = mapper;
    }

    @Override
    public void accumulate(T value) {
        R apply = mapper.apply(value);
        if (apply != null) {
            result.add(apply);
        }
    }

    @Override
    public List<R> get() throws AccumulateException {
        return result;
    }
}
