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

package org.apache.ignite.internal.schema.testobjects;

import static org.apache.ignite.internal.schema.NativeTypes.DATE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;

/**
 * Test object.
 */
@SuppressWarnings("InstanceVariableMayNotBeInitialized")
public class TestObjectWithAllTypes {
    /**
     * @return Random TestObject.
     */
    public static TestObjectWithAllTypes randomObject(Random rnd) {
        final TestObjectWithAllTypes obj = new TestObjectWithAllTypes();
        
        obj.primitiveByteCol = (byte) rnd.nextInt(255);
        obj.primitiveShortCol = (short) rnd.nextInt(65535);
        obj.primitiveIntCol = rnd.nextInt();
        obj.primitiveLongCol = rnd.nextLong();
        obj.primitiveFloatCol = rnd.nextFloat();
        obj.primitiveDoubleCol = rnd.nextDouble();
        
        obj.byteCol = (byte) rnd.nextInt(255);
        obj.shortCol = (short) rnd.nextInt(65535);
        obj.intCol = rnd.nextInt();
        obj.longCol = rnd.nextLong();
        obj.floatCol = rnd.nextFloat();
        obj.doubleCol = rnd.nextDouble();
        
        obj.uuidCol = new UUID(rnd.nextLong(), rnd.nextLong());
        obj.bitmaskCol = IgniteTestUtils.randomBitSet(rnd, 42);
        
        obj.dateCol = (LocalDate) SchemaTestUtils.generateRandomValue(rnd, DATE);
        obj.timeCol = (LocalTime) SchemaTestUtils.generateRandomValue(rnd, NativeTypes.time());
        obj.dateTimeCol = (LocalDateTime) SchemaTestUtils
                .generateRandomValue(rnd, NativeTypes.datetime());
        obj.timestampCol = (Instant) SchemaTestUtils
                .generateRandomValue(rnd, NativeTypes.timestamp());
        
        obj.stringCol = IgniteTestUtils.randomString(rnd, rnd.nextInt(255));
        obj.bytesCol = IgniteTestUtils.randomBytes(rnd, rnd.nextInt(255));
        obj.numberCol = (BigInteger) SchemaTestUtils
                .generateRandomValue(rnd, NativeTypes.numberOf(12));
        obj.decimalCol = (BigDecimal) SchemaTestUtils
                .generateRandomValue(rnd, NativeTypes.decimalOf(19, 3));
        
        obj.nullLongCol = null;
        obj.nullBytesCol = null;
        
        return obj;
    }
    
    // Primitive typed
    private byte primitiveByteCol;
    
    private short primitiveShortCol;
    
    private int primitiveIntCol;
    
    private long primitiveLongCol;
    
    private float primitiveFloatCol;
    
    private double primitiveDoubleCol;
    
    // Reference typed
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
    
    private BitSet bitmaskCol;
    
    private String stringCol;
    
    private byte[] bytesCol;
    
    private byte[] nullBytesCol;
    
    private BigInteger numberCol;
    
    private BigDecimal decimalCol;
    
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
        
        return primitiveByteCol == object.primitiveByteCol
                && primitiveShortCol == object.primitiveShortCol
                && primitiveIntCol == object.primitiveIntCol
                && primitiveLongCol == object.primitiveLongCol
                && Float.compare(object.primitiveFloatCol, primitiveFloatCol) == 0
                && Double.compare(object.primitiveDoubleCol, primitiveDoubleCol) == 0
                && Objects.equals(byteCol, object.byteCol)
                && Objects.equals(shortCol, object.shortCol)
                && Objects.equals(intCol, object.intCol)
                && Objects.equals(longCol, object.longCol)
                && Objects.isNull(nullBytesCol) && Objects.isNull(object.nullBytesCol)
                && Objects.isNull(nullLongCol) && Objects.isNull(object.nullLongCol)
                && Objects.equals(floatCol, object.floatCol)
                && Objects.equals(doubleCol, object.doubleCol)
                && Objects.equals(dateCol, object.dateCol)
                && Objects.equals(timeCol, object.timeCol)
                && Objects.equals(timestampCol, object.timestampCol)
                && Objects.equals(dateTimeCol, object.dateTimeCol)
                && Objects.equals(uuidCol, object.uuidCol)
                && Objects.equals(bitmaskCol, object.bitmaskCol)
                && Objects.equals(stringCol, object.stringCol)
                && Arrays.equals(bytesCol, object.bytesCol)
                && Objects.equals(numberCol, object.numberCol)
                && Objects.equals(decimalCol, object.decimalCol);
    }
    
    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return 73;
    }
    
    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "TestObjectWithAllTypes{" +
                "primitiveByteCol=" + primitiveByteCol +
                ", primitiveShortCol=" + primitiveShortCol +
                ", primitiveIntCol=" + primitiveIntCol +
                ", primitiveLongCol=" + primitiveLongCol +
                ", primitiveFloatCol=" + primitiveFloatCol +
                ", primitiveDoubleCol=" + primitiveDoubleCol +
                ", byteCol=" + byteCol +
                ", shortCol=" + shortCol +
                ", intCol=" + intCol +
                ", longCol=" + longCol +
                ", nullLongCol=" + nullLongCol +
                ", floatCol=" + floatCol +
                ", doubleCol=" + doubleCol +
                ", uuidCol=" + uuidCol +
                ", timeCol=" + timeCol +
                ", dateCol=" + dateCol +
                ", dateTimeCol=" + dateTimeCol +
                ", timestampCol=" + timestampCol +
                ", bitmaskCol=" + bitmaskCol +
                ", stringCol='" + stringCol + '\'' +
                ", bytesCol=" + Arrays.toString(bytesCol) +
                ", nullBytesCol=" + Arrays.toString(nullBytesCol) +
                ", numberCol=" + numberCol +
                ", decimalCol=" + decimalCol +
                '}';
    }
}
