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

import java.util.Objects;
import java.util.StringJoiner;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;

/**
 * Default implementation of {@link Flowable}.
 *
 * @param <T> result type.
 */
public class DefaultFlowable<T> implements Flowable<T> {
    private final T body;

    private final Throwable cause;

    private DefaultFlowable(T body, Throwable cause) {
        this.body = body;
        this.cause = cause;
    }

    @Override
    public Throwable errorCause() {
        return cause;
    }

    @Override
    public T value() {
        return body;
    }

    @Override
    public Class<T> type() {
        return (Class<T>) body.getClass();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultFlowable<?> that = (DefaultFlowable<?>) o;
        return Objects.equals(body, that.body) && Objects.equals(cause, that.cause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(body, cause);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", DefaultFlowable.class.getSimpleName() + "[", "]")
                .add("body=" + body)
                .add("cause=" + cause)
                .toString();
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link DefaultCallOutput.DefaultCallOutputBuilder}.
     */
    public static <T> DefaultFlowOutputBuilder<T> builder() {
        return new DefaultFlowOutputBuilder<>();
    }

    /**
     * Builder of {@link DefaultCallOutput}.
     */
    public static class DefaultFlowOutputBuilder<T> {

        private T body;

        private Throwable cause;

        /**
         * Builder setter.
         *
         * @param body call output body.
         * @return invoked builder instance {@link DefaultCallOutput.DefaultCallOutputBuilder}.
         */
        public DefaultFlowOutputBuilder<T> body(T body) {
            this.body = body;
            return this;
        }

        /**
         * Builder setter.
         *
         * @param cause exception cause.
         * @return invoked builder instance {@link DefaultCallOutput.DefaultCallOutputBuilder}.
         */
        public DefaultFlowOutputBuilder<T> cause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        /**
         * Build method.
         *
         * @return new {@link DefaultCallOutput} with field provided to builder.
         */
        public DefaultFlowable<T> build() {
            return new DefaultFlowable<>(body, cause);
        }
    }
}
