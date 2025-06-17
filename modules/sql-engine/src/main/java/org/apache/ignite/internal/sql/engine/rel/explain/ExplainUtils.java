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

package org.apache.ignite.internal.sql.engine.rel.explain;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.rex.RexVisitor;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.jetbrains.annotations.Nullable;

/** Set of utility method used during EXPLAIN evaluation. */
public final class ExplainUtils {
    private ExplainUtils() {
        throw new AssertionError("Should not be called");
    }

    /** Converts given relation to string representation suitable for EXPLAIN command output. */
    public static String toString(IgniteRel rel) {
        return RelTreeToTextWriter.dumpTree(rel, 0);
    }

    /**
     * Converts the given relational tree to string representation suitable for EXPLAIN command output.
     *
     * @param rel The relation tree to convert.
     * @param padding The number of spaces to use for left-padding the converted tree.
     */
    public static String toString(IgniteRel rel, int padding) {
        return RelTreeToTextWriter.dumpTree(rel, (padding + 1) / RelTreeToTextWriter.NEXT_OPERATOR_INDENT);
    }

    static RexShuttle inputRefRewriter(RelDataType rowType) {
        return new RexShuttle() {
            @Override
            public RexNode visitLocalRef(RexLocalRef ref) {
                return new NamedRexSlot(rowType.getFieldNames().get(ref.getIndex()), ref.getIndex(), ref.getType());
            }

            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return new NamedRexSlot(rowType.getFieldNames().get(ref.getIndex()), ref.getIndex(), ref.getType());
            }
        };
    }

    private static class NamedRexSlot extends RexSlot {
        NamedRexSlot(String name, int index, RelDataType type) {
            super(name, index, type);
        }

        @Override
        public <R> R accept(RexVisitor<R> visitor) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
            throw new AssertionError("Should not be called");
        }

        @Override
        public boolean equals(@Nullable Object obj) {
            return this == obj
                    || obj instanceof NamedRexSlot
                    && index == ((NamedRexSlot) obj).index;
        }

        @Override
        public int hashCode() {
            return index;
        }
    }
}
