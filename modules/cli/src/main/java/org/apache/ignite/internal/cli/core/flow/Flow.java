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

package org.apache.ignite.internal.cli.core.flow;

/**
 * Represents a flow that accepts one flowable and produces a result flowable.
 * Flow shouldn't throw any exception but should return exception as result in output {@link Flowable}.
 *
 * @param <I> Input type.
 * @param <O> Output type.
 */
public interface Flow<I, O> {
    /**
     * Start flow execution.
     *
     * @param input input flowable.
     * @return output flowable.
     */
    Flowable<O> start(Flowable<I> input);

    /**
     * Flow composition method.
     *
     * @param next flow which will be executed after current instance with result of it.
     * @param <OT> new output type.
     * @return output flowable of {@param next}
     */
    default <OT> Flow<I, OT> composite(Flow<O, OT> next) {
        return input -> {
            Flowable<O> outputFlowable = this.start(input);
            return next.start(outputFlowable);
        };
    }
}
