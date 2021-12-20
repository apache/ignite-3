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

package org.apache.ignite.internal.network.serialization;

/**
 * Describes how a class is to be serialized.
 */
public class Serialization {
    /** Serialization type. */
    private final SerializationType type;

    /** Whether a Serializable has writeObject()/readObject()/readObjectNoData() methods. */
    private final boolean hasSerializationOverride;
    /** Whether a Serializable/Externalizable has writeReplace() method. */
    private final boolean hasWriteReplace;
    /** Whether a Serializable/Externalizable has readResolve() method */
    private final boolean hasReadResolve;

    public Serialization(SerializationType type, boolean hasSerializationOverride, boolean hasWriteReplace, boolean hasReadResolve) {
        this.type = type;
        this.hasSerializationOverride = hasSerializationOverride;
        this.hasWriteReplace = hasWriteReplace;
        this.hasReadResolve = hasReadResolve;
    }

    public Serialization(SerializationType type) {
        this(type, false, false, false);
    }

    public SerializationType type() {
        return type;
    }

    public boolean hasSerializationOverride() {
        return hasSerializationOverride;
    }

    public boolean hasWriteReplace() {
        return hasWriteReplace;
    }

    public boolean hasReadResolve() {
        return hasReadResolve;
    }
}
