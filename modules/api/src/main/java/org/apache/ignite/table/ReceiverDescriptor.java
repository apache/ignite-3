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
public class ReceiverDescriptor<A> {
    private final String receiverClassName;

    private final List<DeploymentUnit> units;

    private final @Nullable Marshaller<A, byte[]> argumentMarshaller;

    private ReceiverDescriptor(
            String receiverClassName,
            List<DeploymentUnit> units,
            @Nullable Marshaller<A, byte[]> argumentMarshaller
    ) {
        Objects.requireNonNull(receiverClassName);
        Objects.requireNonNull(units);

        this.receiverClassName = receiverClassName;
        this.units = units;
        this.argumentMarshaller = argumentMarshaller;
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
     * Create a new builder.
     *
     * @return Receiver descriptor builder.
     */
    public static <A> Builder<A> builder(String receiverClassName) {
        Objects.requireNonNull(receiverClassName);

        return new Builder<>(receiverClassName);
    }

    /**
     * Create a new builder.
     *
     * @return Receiver descriptor builder.
     */
    public static <A> Builder<A> builder(Class<? extends DataStreamerReceiver<?, A, ?>> receiverClass) {
        Objects.requireNonNull(receiverClass);

        return new Builder<>(receiverClass.getName());
    }

    public @Nullable Marshaller<A, byte[]> argumentMarshaller() {
        return argumentMarshaller;
    }

    /**
     * Builder.
     */
    public static class Builder<A> {
        private final String receiverClassName;
        private List<DeploymentUnit> units;
        private @Nullable Marshaller<A, byte[]> argumentMarshaller;


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
        public Builder<A> units(List<DeploymentUnit> units) {
            this.units = units;
            return this;
        }

        /**
         * Sets the deployment units.
         *
         * @param units Deployment units.
         * @return This builder.
         */
        public Builder<A> units(DeploymentUnit... units) {
            this.units = List.of(units);
            return this;
        }

        public Builder<A> argumentMarshaller(@Nullable Marshaller<A, byte[]> argumentsMarshaller) {
            this.argumentMarshaller = argumentsMarshaller;
            return this;
        }

        /**
         * Builds the receiver descriptor.
         *
         * @return Receiver descriptor.
         */
        public ReceiverDescriptor<A> build() {
            return new ReceiverDescriptor<>(
                    receiverClassName,
                    units == null ? List.of() : units,
                    argumentMarshaller
            );
        }
    }
}
