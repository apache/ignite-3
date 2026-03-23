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
package org.apache.ignite.raft.jraft.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.raft.jraft.entity.SnapshotMetaImpl;
import org.jetbrains.annotations.Nullable;

/**
 * {@link Marshaller} implementation, based on standard {@link ObjectInputStream} and {@link ObjectOutputStream},
 * that fixes broken compatibility.
 */
public class CompatibleJDKMarshaller implements Marshaller {
    /** Pre-allocated {@link CompatibleJDKMarshaller} instance. */
    public static final Marshaller INSTANCE = new CompatibleJDKMarshaller();

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
    @Override public <T> T unmarshall(ByteBuffer raw) {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(raw.array(), raw.arrayOffset() + raw.position(), raw.remaining());
            ObjectInputStream oos = new CompatibilityObjectInputStream(bais);
            return (T) oos.readObject();
        }
        catch (Exception e) {
            throw new Error(e);
        }
    }

    private static class CompatibilityObjectInputStream extends ObjectInputStream {
        private static final long SERIAL_VERSION_ID_BEFORE_ADDING_SEQUENCE_TOKENS = -6673992096916801742L;
        private static final long SERIAL_VERSION_ID_AFTER_ADDING_SEQUENCE_TOKENS = 7302290474182723103L;

        private boolean readingClassDescriptor;
        private @Nullable String className;

        private CompatibilityObjectInputStream(ByteArrayInputStream bais) throws IOException {
            super(bais);
        }

        @Override
        protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
            readingClassDescriptor = true;

            try {
                return super.readClassDescriptor();
            } finally{
                readingClassDescriptor = false;
            }
        }

        @Override
        public String readUTF() throws IOException {
            String str = super.readUTF();

            if (readingClassDescriptor) {
                // First readUTF() during class descriptor reading yields the class name.
                className = str;
            }

            return str;
        }

        @Override
        public long readLong() throws IOException {
            long value = super.readLong();

            // First readLong() during class descriptor reading yields the serial version ID.
            if (readingClassDescriptor) {
                if (SnapshotMetaImpl.class.getName().equals(className) && value == SERIAL_VERSION_ID_BEFORE_ADDING_SEQUENCE_TOKENS) {
                    value = SERIAL_VERSION_ID_AFTER_ADDING_SEQUENCE_TOKENS;
                }

                className = null;
                readingClassDescriptor = false;
            }

            return value;
        }
    }
}
