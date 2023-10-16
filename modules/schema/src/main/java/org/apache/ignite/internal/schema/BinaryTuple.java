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

package org.apache.ignite.internal.schema;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.lang.InternalTuple;

/**
 * Utility for access to binary tuple elements as typed values and with schema knowledge that allows to read
 * elements as objects.
 */
public class BinaryTuple extends BinaryTupleReader implements InternalTuple {
    /**
     * Constructor.
     *
     * @param elementCount Number of tuple elements.
     * @param bytes Binary tuple.
     */
    public BinaryTuple(int elementCount, byte[] bytes) {
        super(elementCount, bytes);
    }

    /**
     * Constructor.
     *
     * @param elementCount Number of tuple elements.
     * @param buffer Buffer with a binary tuple.
     */
    public BinaryTuple(int elementCount, ByteBuffer buffer) {
        super(elementCount, buffer);
    }
}
