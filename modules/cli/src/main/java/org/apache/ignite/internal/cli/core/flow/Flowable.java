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

import java.util.function.Supplier;

/**
 * Element of {@link Flow} processing.
 *
 * @param <T> result type.
 */
public interface Flowable<T> {

    /**
     * Result check method.
     *
     * @return true if flowable has result.
     */
    default boolean hasResult() {
        return value() != null;
    }

    Class<T> type();

    /**
     * Value provider method.
     *
     * @return Value of the flowable's output.
     */
    T value();

    /**
     * Error check method.
     *
     * @return True if flowable has an error.
     */
    default boolean hasError() {
        return errorCause() != null;
    }

    /**
     * Exception cause provider method.
     *
     * @return the cause of the error.
     */
    Throwable errorCause();

    /**
     * Create interrupted flowable.
     *
     * @param <O> required type.
     * @return nothing.
     */
    static <O> O interrupt() {
        throw new FlowInterruptException();
    }

    /**
     * Transform supplier to Flowable with result.
     *
     * @param supplier value supplier.
     * @param <T> value type.
     * @return {@link Flowable} with supplier result or any exception.
     */
    static <T> Flowable<T> process(Supplier<T> supplier) {
        try {
            return success(supplier.get());
        } catch (FlowInterruptException e) {
            throw e;
        } catch (Exception e) {
            return failure(e);
        }
    }

    /**
     * New successful call output with provided body.
     *
     * @param body for successful call output.
     * @return Successful call output with provided body.
     */
    static <T> Flowable<T> success(T body) {
        return DefaultFlowable.<T>builder()
                .body(body)
                .build();
    }

    /**
     * New failed call output with provided cause.
     *
     * @param cause error of failed call.
     * @return Failed call output with provided cause.
     */
    static <T> Flowable<T> failure(Throwable cause) {
        return DefaultFlowable.<T>builder()
                .cause(cause)
                .build();
    }

    /**
     * New empty call output.
     *
     * @return Empty call output.
     */
    static <T> Flowable<T> empty() {
        return DefaultFlowable.<T>builder()
                .build();
    }
}
