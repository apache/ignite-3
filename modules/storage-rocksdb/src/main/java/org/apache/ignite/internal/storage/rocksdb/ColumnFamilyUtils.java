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

package org.apache.ignite.internal.storage.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.rocksdb.index.RocksDbBinaryTupleComparator;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.internal.type.NativeTypes;
import org.rocksdb.RocksDB;

/**
 * Utilities for converting partition IDs and index names into Column Family names and vice versa.
 */
public class ColumnFamilyUtils {
    static {
        RocksDB.loadLibrary();
    }

    /** Name of the meta column family matches default columns family, meaning that it always exist when new table is created. */
    private static final String META_CF_NAME = "default";

    /** Name of the Column Family that stores partition data. */
    private static final String PARTITION_CF_NAME = "cf-part";

    /** Name of the Column Family that stores garbage collection queue. */
    private static final String GC_QUEUE_CF_NAME = "cf-gc";

    /** Name of the Column Family that stores hash index data. */
    private static final String HASH_INDEX_CF_NAME = "cf-hash";

    /** Prefix for SQL indexes column family names. */
    private static final String SORTED_INDEX_CF_PREFIX = "cf-sorted-";

    /** List of column families names that should always be present in the RocksDB instance. */
    public static final List<byte[]> DEFAULT_CF_NAMES = List.of(
            META_CF_NAME.getBytes(UTF_8),
            PARTITION_CF_NAME.getBytes(UTF_8),
            GC_QUEUE_CF_NAME.getBytes(UTF_8),
            HASH_INDEX_CF_NAME.getBytes(UTF_8)
    );

    /** Nullability flag mask for {@link #sortedIndexCfName(List)}. */
    private static final int NULLABILITY_FLAG = 1;

    /** Order flag mask for {@link #sortedIndexCfName(List)}. */
    private static final int ASC_ORDER_FLAG = 2;

    /** Utility enum to describe a type of the column family - meta or partition. */
    public enum ColumnFamilyType {
        META, PARTITION, GC_QUEUE, HASH_INDEX, SORTED_INDEX, UNKNOWN;

        /**
         * Determines column family type by its name.
         *
         * @param cfName Column family name.
         * @return Column family type.
         */
        public static ColumnFamilyType fromCfName(String cfName) {
            if (META_CF_NAME.equals(cfName)) {
                return META;
            } else if (PARTITION_CF_NAME.equals(cfName)) {
                return PARTITION;
            } else if (GC_QUEUE_CF_NAME.equals(cfName)) {
                return GC_QUEUE;
            } else if (HASH_INDEX_CF_NAME.equals(cfName)) {
                return HASH_INDEX;
            } else if (cfName.startsWith(SORTED_INDEX_CF_PREFIX)) {
                return SORTED_INDEX;
            } else {
                return UNKNOWN;
            }
        }
    }

    /**
     * Converts a {@code byte[]} column family name into an UTF8 string.
     */
    public static String toStringName(byte[] cfName) {
        return new String(cfName, UTF_8);
    }

    /**
     * Generates a sorted index column family name by its columns descriptions.
     * The resulting array has a {@link #SORTED_INDEX_CF_PREFIX} prefix as a UTF8 array, followed by a number of pairs
     * {@code {type, flags}}, where type represents ordinal of the corresponding {@link NativeTypeSpec}, and
     * flags store information about column's nullability and comparison order.
     *
     * @see #comparatorFromCfName(byte[])
     */
    public static byte[] sortedIndexCfName(List<StorageSortedIndexColumnDescriptor> columns) {
        ByteBuffer buf = ByteBuffer.allocate(SORTED_INDEX_CF_PREFIX.length() + columns.size() * 2);

        buf.put(SORTED_INDEX_CF_PREFIX.getBytes(UTF_8));

        for (StorageSortedIndexColumnDescriptor column : columns) {
            NativeType nativeType = column.type();
            NativeTypeSpec nativeTypeSpec = nativeType.spec();

            buf.put((byte) nativeTypeSpec.ordinal());

            int flags = 0;

            if (column.nullable()) {
                flags |= NULLABILITY_FLAG;
            }

            if (column.asc()) {
                flags |= ASC_ORDER_FLAG;
            }

            buf.put((byte) flags);
        }

        return buf.array();
    }

    /**
     * Creates an {@link org.rocksdb.AbstractComparator} instance to compare keys in column family with name {@code cfName}.
     * Please refer to {@link #sortedIndexCfName(List)} for the details of the CF name encoding.
     */
    public static RocksDbBinaryTupleComparator comparatorFromCfName(byte[] cfName) {
        // Length of the string is safe to use here, because it's ASCII anyway.
        int prefixLen = SORTED_INDEX_CF_PREFIX.length();

        List<StorageSortedIndexColumnDescriptor> columns = new ArrayList<>((cfName.length - prefixLen) / 2);

        for (int i = prefixLen; i < cfName.length; i += 2) {
            byte nativeTypeSpecOrdinal = cfName[i];
            byte nativeTypeFlags = cfName[i + 1];

            NativeTypeSpec nativeTypeSpec = NativeTypeSpec.fromOrdinal(nativeTypeSpecOrdinal);

            assert nativeTypeSpec != null : format("Invalid sorted index CF name. [nameBytes={}]", Arrays.toString(cfName));

            NativeType nativeType;

            switch (nativeTypeSpec) {
                case BOOLEAN:
                    nativeType = NativeTypes.BOOLEAN;
                    break;

                case INT8:
                    nativeType = NativeTypes.INT8; // TODO IGNITE-19751 Only use INT64.
                    break;

                case INT16:
                    nativeType = NativeTypes.INT16; // TODO IGNITE-19751 Only use INT64.
                    break;

                case INT32:
                    nativeType = NativeTypes.INT32; // TODO IGNITE-19751 Only use INT64.
                    break;

                case INT64:
                    nativeType = NativeTypes.INT64;
                    break;

                case FLOAT:
                    nativeType = NativeTypes.FLOAT; // TODO IGNITE-19751 Only use DOUBLE? Maybe.
                    break;

                case DOUBLE:
                    nativeType = NativeTypes.DOUBLE;
                    break;

                case DECIMAL:
                    nativeType = NativeTypes.decimalOf(-1, 0);
                    break;

                case UUID:
                    nativeType = NativeTypes.UUID;
                    break;

                case STRING:
                    nativeType = NativeTypes.stringOf(-1);
                    break;

                case BYTES:
                    nativeType = NativeTypes.BYTES;
                    break;

                case BITMASK:
                    nativeType = NativeTypes.bitmaskOf(1);
                    break;

                case NUMBER:
                    nativeType = NativeTypes.numberOf(-1);
                    break;

                case DATE:
                    nativeType = NativeTypes.DATE;
                    break;

                case TIME:
                    nativeType = NativeTypes.time(0);
                    break;

                case DATETIME:
                    nativeType = NativeTypes.datetime(3);
                    break;

                case TIMESTAMP:
                    nativeType = NativeTypes.timestamp(3);
                    break;

                default:
                    throw new AssertionError(format("Unexpected native type. [spec={}]", nativeTypeSpec));
            }

            boolean nullable = (nativeTypeFlags & NULLABILITY_FLAG) != 0;
            boolean asc = (nativeTypeFlags & ASC_ORDER_FLAG) != 0;

            columns.add(new StorageSortedIndexColumnDescriptor("<unknown>", nativeType, nullable, asc));
        }

        return new RocksDbBinaryTupleComparator(columns);
    }
}
