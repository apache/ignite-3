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

package org.apache.ignite.internal.testframework;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.thread.ThreadOperation;

/**
 * Annotation for injecting {@link ExecutorService} instances into tests.
 *
 * <p>Executor is created based on the type of field into which it will need to be injected.</p>
 *
 * <p>For each field a new executor will be created and will have a fixed number of threads.</p>
 *
 * <p>Supported field types:</p>
 * <ul>
 *     <li>{@link ExecutorService}.</li>
 *     <li>{@link ScheduledExecutorService}.</li>
 * </ul>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER})
public @interface InjectExecutorService {
    /**
     * Number of threads in the executor.
     *
     * <p>By default it will depend on the type of executor:</p>
     * <ul>
     *     <li>{@link ScheduledExecutorService} - 1 thread.</li>
     *     <li>{@link ExecutorService} - {@link Runtime#availableProcessors()}.</li>
     * </ul>
     */
    int threadCount() default 0;

    /**
     * Prefix of thread names in the executor.
     *
     * <p>By default the prefix will be in the format "test-class_name-field_name", for example "test-FooTest-commonExecutor" for class
     * fields and in the format "test-class_name-method_name-param_name", for example "test-FooTest-beforeAll-commonExecutor" for class
     * methods.</p>
     */
    String threadPrefix() default "";

    /** Operations that are allowed to be executed on threads. By default, nothing is available. */
    ThreadOperation[] allowedOperations() default {};
}
