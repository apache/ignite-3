/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.internal.network.serialization.ClassDescriptorFactory;
import org.apache.ignite.internal.network.serialization.ClassDescriptorRegistry;
import org.apache.ignite.internal.network.serialization.marshal.DefaultUserObjectMarshaller;
import org.apache.ignite.internal.network.serialization.marshal.MarshalException;
import org.apache.ignite.internal.network.serialization.marshal.UnmarshalException;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;

/**
 *
 */
public class JDKMarshaller implements Marshaller {
    /**
     * {@inheritDoc}
     */
    @Override public byte[] marshall(Object o) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            oos.close();
            return baos.toByteArray();
        }
        catch (Exception e) {
            throw new Error(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public <T> T unmarshall(byte[] raw) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(raw);
            ObjectInputStream oos = new ObjectInputStream(bais);
            return (T) oos.readObject();
        }
        catch (Exception e) {
            throw new Error(e);
        }
    }

    /** */
    public static class NewMarshaller implements Marshaller {
        /** */
        private final ClassDescriptorRegistry registry = new ClassDescriptorRegistry();

        /** */
        private final ClassDescriptorFactory factory = new ClassDescriptorFactory(registry);

        /** */
        private final UserObjectMarshaller uom = new DefaultUserObjectMarshaller(registry, factory);

        /** {@inheritDoc} */
        @Override
        public byte[] marshall(Object o) {
            try {
                return uom.marshal(o).bytes();
            } catch (MarshalException e) {
                throw new RuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override
        public <T> T unmarshall(byte[] raw) {
            try {
                return uom.unmarshal(raw, registry);
            }
            catch (UnmarshalException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
