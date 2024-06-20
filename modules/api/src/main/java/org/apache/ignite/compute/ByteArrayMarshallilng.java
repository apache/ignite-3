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

package org.apache.ignite.compute;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * ttttt.
 */
public class ByteArrayMarshallilng {
    /**
     * ttttt.
     *
     * @param <T> asdf.
     * @return asdf.
     */
    public static <T> byte[] marshal(T object) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(baos)
        ) {
            out.writeObject(object);
            out.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ttsss.
     *
     * @param raw asdf.
     * @param <T> asdf.
     * @return asdf.
     */
    public static <T> T unmarshal(byte[] raw) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(raw);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
