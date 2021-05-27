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

package org.apache.ignite.network.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageSerializationFactory;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * Annotation for interfaces intended to be used as <i>Network Messages</i>. A Network Message is an interface that
 * must satisfy the following requirements:
 *
 * <ol>
 *     <li>It must extend the {@link NetworkMessage} interface either directly or transitively;</li>
 *     <li>It must only contain Ignite-style getter methods that represent the message's properties.</li>
 * </ol>
 *
 * When such interface is marked by this annotation, it can be used by the annotation processor to generate
 * the following classes:
 *
 * <ol>
 *     <li>Builder interface with setters for all declared properties;</li>
 *     <li>An immutable implementation of the message and a nested implementation of the generated builder
 *     for creating new message instances;</li>
 * </ol>
 *
 * If the {@link #autoSerializable} property is set to {@code true}, the annotation processor will additionally generate
 * serialization-related classes:
 *
 * <ol>
 *     <li>{@link MessageSerializer};</li>
 *     <li>{@link MessageDeserializer};</li>
 *     <li>{@link MessageSerializationFactory}.</li>
 * </ol>
 *
 * Properties of messages that can be auto-serialized can only be of <i>directly marshallable type</i>,
 * which is one of the following:
 *
 * <ol>
 *     <li>Primitive type;</li>
 *     <li>{@code String};</li>
 *     <li>{@link UUID};</li>
 *     <li>{@link IgniteUuid};</li>
 *     <li>{@link BitSet};</li>
 *     <li>Nested {@code NetworkMessage};</li>
 *     <li>Array of primitive types, corresponding boxed types or other directly marshallable types;</li>
 *     <li>{@code Collection} of boxed primitive types or other directly marshallable types;</li>
 *     <li>{@code Map} where both keys and values can be of a directly marshallable type.</li>
 * </ol>
 *
 * After all messages in the module have been processed, the processor will use the <i>module descriptor</i> (class
 * annotated with {@link ModuleMessageTypes}) to expose the builders via a
 * Message Factory.
 *
 * @see ModuleMessageTypes
 */
@Target(ElementType.TYPE)
// using the RUNTIME retention policy in order to avoid problems with incremental compilation in an IDE.
@Retention(RetentionPolicy.RUNTIME)
public @interface AutoMessage {
    /**
     * This message's type as described in {@link NetworkMessage#messageType}.
     */
    short value();

    /**
     * When this property is set to {@code true} (default), serialization-related classes will be generated in addition
     * to the message implementation.
     */
    boolean autoSerializable() default true;
}
