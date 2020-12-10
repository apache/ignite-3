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

/**
 * <!-- Package description. -->
 * Contains schema description, tuple assembly and field accessor classes.
 * <p>
 * This package provides necessary infrastructure to create, read, convert to and from POJO classes
 * schema-defined tuples.
 * <p>
 * Schema is defined as a set of columns which are split into key columns chunk and value columns chunk.
 * Each column defined by a name, nullability flag, and a {@link org.apache.ignite.internal.schema.NativeType}.
 * Type is a thin wrapper over the {@link org.apache.ignite.internal.schema.NativeTypeSpec} to provide differentiation
 * between types of one kind with different size (an example of such differentiation is bitmask(n) or number(n)).
 * {@link org.apache.ignite.internal.schema.NativeTypeSpec} provides necessary indirection to read a column as a
 * {@code java.lang.Object} without needing to switch over the column type.
 * <p>
 * A tuple itself does not contain any type metadata and only contains necessary
 * information required for fast column lookup. In a tuple, key columns and value columns are separated
 * and written to chunks with identical structure (so that chunk is self-sufficient, and, provided with
 * the column types can be read independently).
 * Tuple structure has the following format:
 *
 * <pre>
 * +---------+----------+----------+-------------+
 * |  Schema |    Key  | Key chunk | Value chunk |
 * | Version |   Hash  | Bytes     | Bytes       |
 * +---------+------ --+-----------+-------------+
 * | 2 bytes | 4 bytes |                         |
 * +---------+---------+-------------------------+
 * </pre>
 * Each bytes section has the following structure:
 * <pre>
 * +---------+----------+---------+------+--------+--------+
 * |   Total | Vartable |  Varlen | Null | Fixlen | Varlen |
 * |  Length |   Length | Offsets |  Map |  Bytes |  Bytes |
 * +---------+----------+---------+------+--------+--------+
 * | 2 bytes |  2 bytes |                                  |
 * +---------+---------------------------------------------+
 * </pre>
 * To assemble a tuple with some schema, an instance of {@link org.apache.ignite.internal.schema.TupleAssembler}
 * must be used which provides the low-level API for building tuples. When using the tuple assembler, the
 * columns must be passed to the assembler in the internal schema sort order. Additionally, when constructing
 * the instance of the assembler, the user should pre-calculate the size of the tuple to avoid extra array copies,
 * and the number of non-null varlen columns for key and value chunks. Less restrictive building techniques
 * are provided by class (de)serializers and tuple builder, which take care of sizing and column order.
 * <p>
 * To read column values of a tuple, one needs to construct a subclass of
 * {@link org.apache.ignite.internal.schema.Tuple} which provides necessary logic to read arbitrary columns with
 * type checking. For primitive types, {@link org.apache.ignite.internal.schema.Tuple} provides boxed and non-boxed
 * value methods to avoid boxing in scenarios where boxing can be avoided (deserialization of non-null columns to
 * POJO primitives, for example).
 */
package org.apache.ignite.internal.schema;