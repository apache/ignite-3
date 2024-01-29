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

package org.apache.ignite.internal.catalog.serialization;

import java.io.IOException;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Catalog object serializer.
 */
public interface CatalogObjectSerializer<T> {
    /**
     * Reads catalog object from data input.
     *
     * @param version Data format version.
     * @param input Data input.
     * @return Catalog entry.
     */
    T readFrom(int version, IgniteDataInput input) throws IOException;

    /**
     * Writes catalog щиоусе to data output.
     *
     * @param value Catalog entry.
     * @param version Required data format version.
     * @param output Data output.
     */
    void writeTo(T value, int version, IgniteDataOutput output) throws IOException;
}
