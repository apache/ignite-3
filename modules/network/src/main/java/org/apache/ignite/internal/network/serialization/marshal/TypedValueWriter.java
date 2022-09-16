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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.IOException;
import org.apache.ignite.internal.network.serialization.DeclaredType;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * Writes objects to an output taking their declared types into consideration (if any).
 * The type might come from a field or be an array component type.
 */
interface TypedValueWriter {
    /**
     * Writes the given object to the output.
     *
     * @param object        object to write
     * @param declaredType  the declared type of the object; might be {@code null} if the type information is not available
     * @param output        where to write to
     * @param context       marshalling context
     * @throws IOException      if an I/O problem occurs
     * @throws MarshalException if another problem occurs
     */
    void write(Object object, @Nullable DeclaredType declaredType, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException;
}
