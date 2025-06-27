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

package org.apache.ignite.internal.manager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

/**
 * Component lifecycle context.
 */
public class ComponentContext {

    /**
     * The executor that will execute the async part of start or stop.
     */
    private final ExecutorService executor;

    /**
     * Ctor.
     *
     * @param executor The executor that will execute the async part of start or stop.
     */
    public ComponentContext(ExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Constructor with common pool.
     */
    public ComponentContext() {
        this(ForkJoinPool.commonPool());
    }

    /**
     * Gets the executor that will execute the async part of start or stop.
     *
     * @return The executor that will execute the async part of start or stop.
     */
    public ExecutorService executor() {
        return executor;
    }

}
