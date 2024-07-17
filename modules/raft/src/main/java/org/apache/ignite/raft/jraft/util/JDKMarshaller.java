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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.raft.Marshaller;

/**
 * {@link Marshaller} implementation, based on standard {@link ObjectInputStream} and {@link ObjectOutputStream}.
 */
public class JDKMarshaller implements Marshaller {
    /** Pre-allocated {@link JDKMarshaller} instance. */
    public static final Marshaller INSTANCE = new JDKMarshaller();

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
            ObjectInputStream oos = new ObjectInputStream(bais);
            return (T) oos.readObject();
        }
        catch (Exception e) {
            throw new Error(e);
        }
    }
}
