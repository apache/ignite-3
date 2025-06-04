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

package org.apache.ignite.table;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Data streamer receiver descriptor.
 */
public class DataStreamerReceiverDescriptor<T, A, R> {
    private final String receiverClassName;

    private final List<DeploymentUnit> units;

    private final ReceiverExecutionOptions options;

    private final @Nullable Marshaller<T, byte[]> payloadMarshaller;

    private final @Nullable Marshaller<A, byte[]> argumentMarshaller;

    private final @Nullable Marshaller<R, byte[]> resultMarshaller;

    private DataStreamerReceiverDescriptor(
            String receiverClassName,
            List<DeploymentUnit> units,
            ReceiverExecutionOptions options,
            @Nullable Marshaller<T, byte[]> payloadMarshaller,
            @Nullable Marshaller<A, byte[]> argumentMarshaller,
            @Nullable Marshaller<R, byte[]> resultMarshaller
    ) {
        Objects.requireNonNull(receiverClassName);
        Objects.requireNonNull(units);

        this.receiverClassName = receiverClassName;
        this.units = units;
        this.options = options;
        this.payloadMarshaller = payloadMarshaller;
        this.argumentMarshaller = argumentMarshaller;
        this.resultMarshaller = resultMarshaller;
    }

    /**
     * Streamer receiver class name.
     *
     * @return Streamer receiver class name.
     */
    public String receiverClassName() {
        return receiverClassName;
    }

    /**
     * Deployment units.
     *
     * @return Deployment units.
     */
    public List<DeploymentUnit> units() {
        return units;
    }

    /**
     * Receiver execution options.
     *
     * @return Receiver execution options.
     */
    public ReceiverExecutionOptions options() {
        return options;
    }

    /**
     * Payload marshaller.
     *
     * @return Payload marshaller.
     */
    public @Nullable Marshaller<T, byte[]> payloadMarshaller() {
        return payloadMarshaller;
    }

    /**
     * Argument marshaller.
     *
     * @return Argument marshaller.
     */
    public @Nullable Marshaller<A, byte[]> argumentMarshaller() {
        return argumentMarshaller;
    }

    /**
     * Result marshaller.
     *
     * @return Result marshaller.
     */
    public @Nullable Marshaller<R, byte[]> resultMarshaller() {
        return resultMarshaller;
    }

    /**
     * Create a new builder.
     *
     * @return Receiver descriptor builder.
     */
    public static <T, A, R> Builder<T, A, R> builder(String receiverClassName) {
        Objects.requireNonNull(receiverClassName);

        return new Builder<>(receiverClassName);
    }

    /**
     * Create a new builder.
     *
     * @return Receiver descriptor builder.
     */
    public static <T, A, R> Builder<T, A, R> builder(Class<? extends DataStreamerReceiver<T, A, R>> receiverClass) {
        Objects.requireNonNull(receiverClass);

        return new Builder<>(receiverClass.getName());
    }

    /**
     * Create a new builder from a receiver instance.
     * Populates {@link #payloadMarshaller()}, {@link #argumentMarshaller()}, and {@link #resultMarshaller()} from the provided receiver.
     *
     * @return Receiver descriptor builder.
     */
    public static <T, A, R> Builder<T, A, R> builder(DataStreamerReceiver<T, A, R> receiver) {
        Objects.requireNonNull(receiver);

        return new Builder<T, A, R>(receiver.getClass().getName())
                .payloadMarshaller(receiver.payloadMarshaller())
                .argumentMarshaller(receiver.argumentMarshaller())
                .resultMarshaller(receiver.resultMarshaller());
    }

    /**
     * Builder.
     */
    public static class Builder<T, A, R> {
        private final String receiverClassName;
        private List<DeploymentUnit> units;
        private @Nullable Marshaller<T, byte[]> payloadMarshaller;
        private @Nullable Marshaller<A, byte[]> argumentMarshaller;
        private @Nullable Marshaller<R, byte[]> resultMarshaller;
        private ReceiverExecutionOptions options = ReceiverExecutionOptions.DEFAULT;

        private Builder(String receiverClassName) {
            Objects.requireNonNull(receiverClassName);

            this.receiverClassName = receiverClassName;
        }

        /**
         * Sets the deployment units.
         *
         * @param units Deployment units.
         * @return This builder.
         */
        public Builder<T, A, R> units(List<DeploymentUnit> units) {
            this.units = units;
            return this;
        }

        /**
         * Sets the deployment units.
         *
         * @param units Deployment units.
         * @return This builder.
         */
        public Builder<T, A, R> units(DeploymentUnit... units) {
            this.units = List.of(units);
            return this;
        }

        /**
         * Sets the receiver execution options.
         *
         * @param options Receiver execution options.
         * @return This builder.
         */
        public Builder<T, A, R> options(ReceiverExecutionOptions options) {
            this.options = options;
            return this;
        }

        /**
         * Sets the payload marshaller.
         *
         * @param payloadMarshaller Payload marshaller.
         * @return This builder.
         */
        public Builder<T, A, R> payloadMarshaller(@Nullable Marshaller<T, byte[]> payloadMarshaller) {
            this.payloadMarshaller = payloadMarshaller;
            return this;
        }

        /**
         * Sets the argument marshaller.
         *
         * @param argumentMarshaller Argument marshaller.
         * @return This builder.
         */
        public Builder<T, A, R> argumentMarshaller(@Nullable Marshaller<A, byte[]> argumentMarshaller) {
            this.argumentMarshaller = argumentMarshaller;
            return this;
        }

        /**
         * Sets the result marshaller.
         *
         * @param resultMarshaller Result marshaller.
         * @return This builder.
         */
        public Builder<T, A, R> resultMarshaller(@Nullable Marshaller<R, byte[]> resultMarshaller) {
            this.resultMarshaller = resultMarshaller;
            return this;
        }

        /**
         * Builds the receiver descriptor.
         *
         * @return Receiver descriptor.
         */
        public DataStreamerReceiverDescriptor<T, A, R> build() {
            return new DataStreamerReceiverDescriptor<>(
                    receiverClassName,
                    units != null ? units : List.of(),
                    options,
                    payloadMarshaller,
                    argumentMarshaller,
                    resultMarshaller
            );
        }
    }
}
