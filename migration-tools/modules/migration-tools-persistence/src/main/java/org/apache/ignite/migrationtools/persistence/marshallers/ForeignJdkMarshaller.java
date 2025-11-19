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

package org.apache.ignite.migrationtools.persistence.marshallers;

import java.io.InputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/** {@link JdkMarshaller} implementation which uses {@link ForeignObjectInputStream}. */
public class ForeignJdkMarshaller extends JdkMarshaller {
    @Override
    protected <T> T unmarshal0(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        // Essentially the same impl as JdkMarshaller but with a different underlying ObjectInputStream.
        assert in != null;

        ClassLoader localClassLoader = (clsLdr != null) ? clsLdr : getClass().getClassLoader();

        try (var objIn = new ForeignObjectInputStream(in, localClassLoader)) {
            return (T) objIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new IgniteCheckedException("Failed to find class with given class loader for unmarshalling "
                    + "[clsLdr=" + localClassLoader + ", cls=" + e.getMessage() + "]", e);
        } catch (Exception e) {
            throw new IgniteCheckedException("Failed to deserialize object with given class loader: " + localClassLoader, e);
        }
    }
}
