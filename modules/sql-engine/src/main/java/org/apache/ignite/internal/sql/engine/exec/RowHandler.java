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

package org.apache.ignite.internal.sql.engine.exec;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.schema.BinaryTuple;

/**
 * Universal accessor for rows. It also has factory methods.
 */
public interface RowHandler<RowT> extends RowAccessor<RowT> {
    /**
     * Assembly row representation as BinaryTuple.
     *
     * @param row Incoming data to be processed.
     * @return {@link BinaryTuple} representation.
     */
    BinaryTuple toBinaryTuple(RowT row);

    default ByteBuffer toByteBuffer(RowT row) {
        return toBinaryTuple(row).byteBuffer();
    }

    /** String representation. */
    String toString(RowT row);
}
