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

package org.apache.ignite.internal.schema;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.row.InternalTuple;

/**
 * Utility for access to binary tuple elements as typed values and with schema knowledge that allows to read
 * elements as objects.
 */
public class BinaryTuple extends BinaryTupleReader implements InternalTuple {
    /** Tuple schema. */
    private final BinaryTupleSchema schema;

    /**
     * Constructor.
     *
     * @param schema Tuple schema.
     * @param bytes Binary tuple.
     */
    public BinaryTuple(BinaryTupleSchema schema, byte[] bytes) {
        super(schema.elementCount(), bytes);
        this.schema = schema;
    }

    /**
     * Constructor.
     *
     * @param schema Tuple schema.
     * @param buffer Buffer with a binary tuple.
     */
    public BinaryTuple(BinaryTupleSchema schema, ByteBuffer buffer) {
        super(schema.elementCount(), buffer);
        this.schema = schema;
    }

    /** {@inheritDoc} */
    @Override
    public int count() {
        return elementCount();
    }

    /** {@inheritDoc} */
    @Override
    public BigDecimal decimalValue(int index) {
        return decimalValue(index, schema.element(index).decimalScale);
    }

    /**
     * Reads value for specified element.
     *
     * @param index Element index.
     * @return Element value.
     */
    public Object value(int index) {
        return schema.element(index).typeSpec.objectValue(this, index);
    }
}
