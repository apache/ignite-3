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

package org.apache.ignite.internal.marshaller.testobjects;

import static org.apache.ignite.internal.schema.SchemaTestUtils.generateRandomValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.type.NativeTypes;

/**
 * Test object.
 */
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class TestObjectWithAllTypes {
    /**
     * Creates an object with random data.
     */
    public static TestObjectWithAllTypes randomObject(Random rnd) {
        TestObjectWithAllTypes obj = new TestObjectWithAllTypes();

        obj.primitiveBooleanCol = (boolean) generateRandomValue(rnd, NativeTypes.BOOLEAN);
        obj.primitiveByteCol = (byte) generateRandomValue(rnd, NativeTypes.INT8);
        obj.primitiveShortCol = (short) generateRandomValue(rnd, NativeTypes.INT16);
        obj.primitiveIntCol = (int) generateRandomValue(rnd, NativeTypes.INT32);
        obj.primitiveLongCol = (long) generateRandomValue(rnd, NativeTypes.INT64);
        obj.primitiveFloatCol = (float) generateRandomValue(rnd, NativeTypes.FLOAT);
        obj.primitiveDoubleCol = (double) generateRandomValue(rnd, NativeTypes.DOUBLE);

        obj.booleanCol = (boolean) generateRandomValue(rnd, NativeTypes.BOOLEAN);
        obj.byteCol = (byte) generateRandomValue(rnd, NativeTypes.INT8);
        obj.shortCol = (short) generateRandomValue(rnd, NativeTypes.INT16);
        obj.intCol = (int) generateRandomValue(rnd, NativeTypes.INT32);
        obj.longCol = (long) generateRandomValue(rnd, NativeTypes.INT64);
        obj.floatCol = (float) generateRandomValue(rnd, NativeTypes.FLOAT);
        obj.doubleCol = (double) generateRandomValue(rnd, NativeTypes.DOUBLE);

        obj.uuidCol = (UUID) generateRandomValue(rnd, NativeTypes.UUID);

        obj.dateCol = (LocalDate) generateRandomValue(rnd, NativeTypes.DATE);
        obj.timeCol = (LocalTime) generateRandomValue(rnd, NativeTypes.time(0));
        obj.dateTimeCol = (LocalDateTime) generateRandomValue(rnd, NativeTypes.datetime(0));
        obj.timestampCol = (Instant) generateRandomValue(rnd, NativeTypes.timestamp(6));

        obj.stringCol = (String) generateRandomValue(rnd, NativeTypes.stringOf(rnd.nextInt(255)));
        obj.bytesCol = (byte[]) generateRandomValue(rnd, NativeTypes.blobOf(255));
        obj.decimalCol = (BigDecimal) generateRandomValue(rnd, NativeTypes.decimalOf(10, 3));

        obj.nullLongCol = null;
        obj.nullBytesCol = null;

        return obj;
    }

    /**
     * Creates an object with random data.
     */
    public static TestObjectWithAllTypes randomKey(Random rnd) {
        final TestObjectWithAllTypes obj = randomObject(rnd);

        obj.nullLongCol = rnd.nextLong();
        obj.nullBytesCol = IgniteTestUtils.randomBytes(rnd, rnd.nextInt(255));

        return obj;
    }

    // Primitive typed
    private boolean primitiveBooleanCol;

    private byte primitiveByteCol;

    private short primitiveShortCol;

    private int primitiveIntCol;

    private long primitiveLongCol;

    private float primitiveFloatCol;

    private double primitiveDoubleCol;

    // Reference typed
    private Boolean booleanCol;

    private Byte byteCol;

    private Short shortCol;

    private Integer intCol;

    private Long longCol;

    private Long nullLongCol;

    private Float floatCol;

    private Double doubleCol;

    private UUID uuidCol;

    private LocalTime timeCol;

    private LocalDate dateCol;

    private LocalDateTime dateTimeCol;

    private Instant timestampCol;

    private String stringCol;

    private byte[] bytesCol;

    private byte[] nullBytesCol;

    private BigDecimal decimalCol;

    private static int staticField;

    private transient int transientField;

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TestObjectWithAllTypes object = (TestObjectWithAllTypes) o;

        return primitiveBooleanCol == object.primitiveBooleanCol
                && primitiveByteCol == object.primitiveByteCol
                && primitiveShortCol == object.primitiveShortCol
                && primitiveIntCol == object.primitiveIntCol
                && primitiveLongCol == object.primitiveLongCol
                && Float.compare(object.primitiveFloatCol, primitiveFloatCol) == 0
                && Double.compare(object.primitiveDoubleCol, primitiveDoubleCol) == 0
                && Objects.equals(booleanCol, object.booleanCol)
                && Objects.equals(byteCol, object.byteCol)
                && Objects.equals(shortCol, object.shortCol)
                && Objects.equals(intCol, object.intCol)
                && Objects.equals(longCol, object.longCol)
                && Arrays.equals(nullBytesCol, object.nullBytesCol)
                && Objects.equals(nullLongCol, object.nullLongCol)
                && Objects.equals(floatCol, object.floatCol)
                && Objects.equals(doubleCol, object.doubleCol)
                && Objects.equals(dateCol, object.dateCol)
                && Objects.equals(timeCol, object.timeCol)
                && Objects.equals(timestampCol, object.timestampCol)
                && Objects.equals(dateTimeCol, object.dateTimeCol)
                && Objects.equals(uuidCol, object.uuidCol)
                && Objects.equals(stringCol, object.stringCol)
                && Arrays.equals(bytesCol, object.bytesCol)
                && Objects.equals(decimalCol, object.decimalCol);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return 73;
    }

    public boolean getPrimitiveBooleanCol() {
        return primitiveBooleanCol;
    }

    public void setPrimitiveBooleanCol(boolean primitiveBooleanCol) {
        this.primitiveBooleanCol = primitiveBooleanCol;
    }

    public byte getPrimitiveByteCol() {
        return primitiveByteCol;
    }

    public void setPrimitiveByteCol(byte primitiveByteCol) {
        this.primitiveByteCol = primitiveByteCol;
    }

    public short getPrimitiveShortCol() {
        return primitiveShortCol;
    }

    public void setPrimitiveShortCol(short primitiveShortCol) {
        this.primitiveShortCol = primitiveShortCol;
    }

    public int getPrimitiveIntCol() {
        return primitiveIntCol;
    }

    public void setPrimitiveIntCol(int primitiveIntCol) {
        this.primitiveIntCol = primitiveIntCol;
    }

    public long getPrimitiveLongCol() {
        return primitiveLongCol;
    }

    public void setPrimitiveLongCol(long primitiveLongCol) {
        this.primitiveLongCol = primitiveLongCol;
    }

    public float getPrimitiveFloatCol() {
        return primitiveFloatCol;
    }

    public void setPrimitiveFloatCol(float primitiveFloatCol) {
        this.primitiveFloatCol = primitiveFloatCol;
    }

    public double getPrimitiveDoubleCol() {
        return primitiveDoubleCol;
    }

    public void setPrimitiveDoubleCol(double primitiveDoubleCol) {
        this.primitiveDoubleCol = primitiveDoubleCol;
    }

    public Boolean getBooleanCol() {
        return booleanCol;
    }

    public void setBooleanCol(Boolean booleanCol) {
        this.booleanCol = booleanCol;
    }

    public Byte getByteCol() {
        return byteCol;
    }

    public void setByteCol(Byte byteCol) {
        this.byteCol = byteCol;
    }

    public Short getShortCol() {
        return shortCol;
    }

    public void setShortCol(Short shortCol) {
        this.shortCol = shortCol;
    }

    public Integer getIntCol() {
        return intCol;
    }

    public void setIntCol(Integer intCol) {
        this.intCol = intCol;
    }

    public Long getLongCol() {
        return longCol;
    }

    public void setLongCol(Long longCol) {
        this.longCol = longCol;
    }

    public Long getNullLongCol() {
        return nullLongCol;
    }

    public void setNullLongCol(Long nullLongCol) {
        this.nullLongCol = nullLongCol;
    }

    public Float getFloatCol() {
        return floatCol;
    }

    public void setFloatCol(Float floatCol) {
        this.floatCol = floatCol;
    }

    public Double getDoubleCol() {
        return doubleCol;
    }

    public void setDoubleCol(Double doubleCol) {
        this.doubleCol = doubleCol;
    }

    public UUID getUuidCol() {
        return uuidCol;
    }

    public void setUuidCol(UUID uuidCol) {
        this.uuidCol = uuidCol;
    }

    public LocalTime getTimeCol() {
        return timeCol;
    }

    public void setTimeCol(LocalTime timeCol) {
        this.timeCol = timeCol;
    }

    public LocalDate getDateCol() {
        return dateCol;
    }

    public void setDateCol(LocalDate dateCol) {
        this.dateCol = dateCol;
    }

    public LocalDateTime getDateTimeCol() {
        return dateTimeCol;
    }

    public void setDateTimeCol(LocalDateTime dateTimeCol) {
        this.dateTimeCol = dateTimeCol;
    }

    public Instant getTimestampCol() {
        return timestampCol;
    }

    public void setTimestampCol(Instant timestampCol) {
        this.timestampCol = timestampCol;
    }

    public String getStringCol() {
        return stringCol;
    }

    public void setStringCol(String stringCol) {
        this.stringCol = stringCol;
    }

    public byte[] getBytesCol() {
        return bytesCol;
    }

    public void setBytesCol(byte[] bytesCol) {
        this.bytesCol = bytesCol;
    }

    public byte[] getNullBytesCol() {
        return nullBytesCol;
    }

    public void setNullBytesCol(byte[] nullBytesCol) {
        this.nullBytesCol = nullBytesCol;
    }

    public BigDecimal getDecimalCol() {
        return decimalCol;
    }

    public void setDecimalCol(BigDecimal decimalCol) {
        this.decimalCol = decimalCol;
    }
}
