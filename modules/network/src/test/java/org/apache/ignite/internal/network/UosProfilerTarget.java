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

package org.apache.ignite.internal.network;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.network.SerializationMicroBenchmark.TestClass;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.network.serialization.marshal.MarshalException;
import org.apache.ignite.internal.network.serialization.marshal.MarshalledObject;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;

/**
 * For running with a profiler.
 */
public class UosProfilerTarget {

    static Object obj;

    static final ClassDescriptorRegistry registry = new ClassDescriptorRegistry();
    static UserObjectMarshaller userObjectMarshaller;

    static Class<?> clz;

    static byte[] marshalledByUos;

    static {
        var factory = new ClassDescriptorFactory(registry);
        userObjectMarshaller = new DefaultUserObjectMarshaller(registry, factory);
        // obj = AllTypesMessageGenerator.generate(10, true, true);
        obj = AllTypesMessageGenerator.generate(10, true, false);
        // obj = new TestClass(1000, false, 3000);
        clz = obj.getClass();
        factory.create(clz);
        try {
            MarshalledObject marshal = userObjectMarshaller.marshal(obj);
            marshalledByUos = marshal.bytes();
        } catch (MarshalException e) {
            e.printStackTrace(); // NOPMD
        }
    }

    public static void main(String[] args) throws Exception {
        // serialize();
        deserialize();
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod") // Used manually by toggling the commented-out call in main()
    private static void serialize() throws Exception {
        int accumulatedCode = 1;
        int count = (obj instanceof TestClass) ? 30_000_000 : 100_000;
        for (int i = 0; i < count; i++) {
            byte[] bytes = doMarshal(obj);
            int code = Arrays.hashCode(bytes);
            accumulatedCode ^= code;
        }

        System.out.println(accumulatedCode);
    }

    private static byte[] doMarshal(Object obj1) throws Exception {
        MarshalledObject marshal = userObjectMarshaller.marshal(obj1);
        return marshal.bytes();
    }

    private static void deserialize() throws Exception {
        int accumulatedCode = 1;
        int count = (obj instanceof TestClass) ? 30_000_000 : 100_000;
        for (int i = 0; i < count; i++) {
            Object object = doUnmarshal(marshalledByUos);
            int code = Objects.hashCode(object);
            accumulatedCode ^= code;
        }

        System.out.println(accumulatedCode);
    }

    private static Object doUnmarshal(byte[] bytes) throws Exception {
        return userObjectMarshaller.unmarshal(bytes, registry);
    }
}
