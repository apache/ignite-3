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

package org.apache.ignite.internal.versioned;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.jetbrains.annotations.Nullable;

/**
 * Serializes and deserializes objects in a versioned way: that is, includes version to make it possible to deserialize objects serialized
 * (and persisted) in earlier versions later when the corresponding class'es structure has changed.
 *
 * <p>It is supposed to be used for the cases when some object needs to be persisted to be stored for some time (maybe, a long time).
 *
 * <p>If a format is changed (for example, a new field is added), the version returned by {@link #getProtocolVersion()} has
 * to be incremented.
 */
public abstract class VersionedSerializer<T> {
    /** Magic number to detect correct serialized objects. */
    private static final int MAGIC = 0x43BEEF00;

    /**
     * Returns protocol version.
     */
    protected byte getProtocolVersion() {
        return 1;
    }

    /**
     * Save object's specific (that is, ignoring the signature and version) data content.
     *
     * @param object object to write.
     * @param out Output to write data content.
     * @throws IOException If an I/O error occurs.
     */
    protected abstract void writeExternalData(T object, IgniteDataOutput out) throws IOException;

    /**
     * Writes an object to an output, including a signature and version.
     *
     * @param object Object to write.
     * @param out Output to which to write.
     * @throws IOException If an I/O error occurs.
     */
    public final void writeExternal(T object, IgniteDataOutput out) throws IOException {
        int hdr = MAGIC + Byte.toUnsignedInt(getProtocolVersion());

        out.writeInt(hdr);

        writeExternalData(object, out);
    }

    /**
     * Load object's specific data content.
     *
     * @param protoVer Input object version.
     * @param in Input to load data content.
     * @throws IOException If an I/O error occurs.
     */
    protected abstract T readExternalData(byte protoVer, IgniteDataInput in) throws IOException;

    /**
     * Reads an object which was earlier saved with {@link #writeExternal(Object, IgniteDataOutput)}.
     *
     * <p>Signature is checked when reading.
     *
     * @param in Input from which to read.
     * @throws IOException If an I/O error occurs.
     * @see #writeExternal(Object, IgniteDataOutput)
     */
    public final T readExternal(IgniteDataInput in) throws IOException {
        int hdr = in.readInt();

        if ((hdr & MAGIC) != MAGIC) {
            throw new IOException("Unexpected serialized object header " + "[actual=" + Integer.toHexString(hdr)
                    + ", expected=" + Integer.toHexString(MAGIC) + "]");
        }

        byte ver = (byte) (hdr & 0xFF);

        return readExternalData(ver, in);
    }

    protected static void writeNullableString(@Nullable String str, IgniteDataOutput out) throws IOException {
        out.writeVarInt(str == null ? -1 : str.length());
        if (str != null) {
            out.writeByteArray(str.getBytes(UTF_8));
        }
    }

    protected static @Nullable String readNullableString(IgniteDataInput in) throws IOException {
        int lengthOrMinusOne = in.readVarIntAsInt();
        if (lengthOrMinusOne == -1) {
            return null;
        }

        return new String(in.readByteArray(lengthOrMinusOne), UTF_8);
    }

    protected static void writeStringSet(Set<String> strings, IgniteDataOutput out) throws IOException {
        out.writeVarInt(strings.size());
        for (String str : strings) {
            out.writeUTF(str);
        }
    }

    protected static Set<String> readStringSet(IgniteDataInput in) throws IOException {
        int size = in.readVarIntAsInt();

        Set<String> result = new HashSet<>(IgniteUtils.capacity(size));
        for (int i = 0; i < size; i++) {
            result.add(in.readUTF());
        }

        return result;
    }

    protected static void writeVarIntSet(Set<Integer> varIntSet, IgniteDataOutput out) throws IOException {
        out.writeVarInt(varIntSet.size());

        for (int partitionId : varIntSet) {
            out.writeVarInt(partitionId);
        }
    }

    protected static Set<Integer> readVarIntSet(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Set<Integer> set = new HashSet<>(IgniteUtils.capacity(length));
        for (int i = 0; i < length; i++) {
            set.add(in.readVarIntAsInt());
        }

        return set;
    }
}
