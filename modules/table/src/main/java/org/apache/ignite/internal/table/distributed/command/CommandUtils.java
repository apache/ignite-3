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

package org.apache.ignite.internal.table.distributed.command;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * This is an utility class for serialization cache tuples. It will be removed after another way for serialization is implemented into the
 * network layer.
 * TODO: Remove it after (IGNITE-14793)
 */
public class CommandUtils {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CommandUtils.class);

    /**
     * Writes a list of rows to byte array.
     *
     * @param rows     Collection of rows.
     * @return         Rows data.
     */
    public static byte[] rowsToBytes(Collection<BinaryRow> rows) {
        if (rows == null || rows.isEmpty()) {
            return null;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            for (BinaryRow row : rows) {
                if (row == null) {
                    baos.write(intToBytes(0));
                } else {
                    byte[] bytes = rowToBytes(row);

                    baos.write(intToBytes(bytes.length));
                    baos.write(bytes);
                }
            }

            baos.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            LOG.debug("unable to write rows to stream [rowsCount={}]", e, rows.size());

            throw new IgniteInternalException(Common.UNEXPECTED_ERR, e);
        }
    }

    /**
     * Writes a row to byte array.
     *
     * @param row      Row.
     * @return         Row bytes.
     */
    public static byte[] rowToBytes(@Nullable BinaryRow row) {
        if (row == null) {
            return null;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            row.writeTo(baos);

            baos.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            LOG.debug("Unable to write row to stream [row={}]", e, row);

            throw new IgniteInternalException(Common.UNEXPECTED_ERR, e);
        }
    }

    /**
     * Reads the keys from a byte array.
     *
     * @param bytes    Byte array.
     * @param consumer Consumer for binary row.
     */
    public static void readRows(byte[] bytes, Consumer<BinaryRow> consumer) {
        if (bytes == null || bytes.length == 0) {
            return;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            byte[] lenBytes = new byte[4];

            byte[] rowBytes;

            int read;

            while ((read = bais.read(lenBytes)) != -1) {
                assert read == 4;

                int len = bytesToInt(lenBytes);

                if (len == 0) {
                    consumer.accept(null);

                    continue;
                }

                rowBytes = new byte[len];

                read = bais.read(rowBytes);

                assert read == len;

                consumer.accept(new ByteBufferRow(rowBytes));
            }
        } catch (IOException e) {
            LOG.debug("Unable to read rows from stream", e);

            throw new IgniteInternalException(Common.UNEXPECTED_ERR, e);
        }
    }

    /**
     * Writes a row map to byte array.
     *
     * @param map Map a row id to the binary row.
     * @return Array of bytes.
     */
    public static byte[] rowMapToBytes(Map<RowId, BinaryRow> map) {
        if (map == null) {
            return null;
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeInt(map.size());

                for (Map.Entry<RowId, BinaryRow> e : map.entrySet()) {
                    oos.writeShort((short) e.getKey().partitionId());
                    oos.writeLong(e.getKey().mostSignificantBits());
                    oos.writeLong(e.getKey().leastSignificantBits());

                    if (e.getValue() == null) {
                        oos.writeInt(0);
                    } else {
                        byte[] bytes = rowToBytes(e.getValue());

                        oos.writeInt(bytes.length);
                        oos.write(bytes);
                    }
                }

            }

            baos.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            LOG.debug("Unable to write the row map to stream [map={}]", e, map);

            throw new IgniteInternalException(Common.UNEXPECTED_ERR, e);
        }
    }

    /**
     * Reads a row map from bytes.
     *
     * @param bytes Bytes to read.
     * @param consumer Consumer for a key and a value for the map entry.
     */
    public static void readRowMap(byte[] bytes, BiConsumer<RowId, BinaryRow> consumer) {
        if (bytes == null || bytes.length == 0) {
            return;
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream ois = new ObjectInputStream(bais)) {
                int size = ois.readInt();

                for (int i = 0; i < size; i++) {
                    int  partId = ois.readShort() & 0xFFFF;
                    long mostSignificantBits = ois.readLong();
                    long leastSignificantBits = ois.readLong();

                    RowId id = new RowId(partId, mostSignificantBits, leastSignificantBits);

                    int len = ois.readInt();

                    BinaryRow row = null;

                    if (len != 0) {
                        byte[] rowBytes = new byte[len];

                        int read = ois.read(rowBytes);

                        assert read == len;

                        row = new ByteBufferRow(rowBytes);
                    }

                    consumer.accept(id, row);
                }
            }
        } catch (IOException e) {
            LOG.debug("Unable to read row map from stream [bytes={}]", e, bytes);

            throw new IgniteInternalException(Common.UNEXPECTED_ERR, e);
        }
    }

    /**
     * Serializes an integer to the byte array.
     *
     * @param i Integer value.
     * @return Byte array.
     */
    private static byte[] intToBytes(int i) {
        byte[] arr = new byte[4];
        ByteBuffer.wrap(arr).putInt(i);
        return arr;
    }

    /**
     * Deserializes a byte array to the integer.
     *
     * @param bytes Byte array.
     * @return Integer value.
     */
    private static int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }
}
