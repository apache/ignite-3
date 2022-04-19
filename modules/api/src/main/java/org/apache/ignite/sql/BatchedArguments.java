/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents query arguments for batch query execution.
 *
 * <p>TODO: replace inheritance with delegation.
 * TODO: add arguments length validation.
 * TODO: add named arguments support.
 */
public class BatchedArguments extends ArrayList<List<Object>> implements List<List<Object>> {
    /**
     * Creates batched arguments.
     *
     * @param args Arguments.
     * @return Batch query arguments.
     */
    public static BatchedArguments of(Object... args) {
        BatchedArguments arguments = new BatchedArguments();

        arguments.add(Arrays.asList(args));

        return arguments;
    }

    /**
     * Creates batched arguments.
     *
     * @param args Arguments.
     * @return Batch query arguments.
     */
    public static BatchedArguments of(List<List<Object>> args) {
        BatchedArguments arguments = new BatchedArguments();

        arguments.addAll(args);

        return arguments;
    }

    /**
     * Appends arguments to the batch.
     *
     * @param args Arguments.
     * @return {@code this} for chaining.
     */
    public BatchedArguments add(Object... args) {
        add(List.of(args));

        return this;
    }
}
