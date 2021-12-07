package org.apache.ignite.internal.marshaller;

import static org.apache.ignite.internal.marshaller.BinaryMode.BITSET;
import static org.apache.ignite.internal.marshaller.BinaryMode.BYTE;
import static org.apache.ignite.internal.marshaller.BinaryMode.BYTE_ARR;
import static org.apache.ignite.internal.marshaller.BinaryMode.DATE;
import static org.apache.ignite.internal.marshaller.BinaryMode.DATETIME;
import static org.apache.ignite.internal.marshaller.BinaryMode.DECIMAL;
import static org.apache.ignite.internal.marshaller.BinaryMode.DOUBLE;
import static org.apache.ignite.internal.marshaller.BinaryMode.FLOAT;
import static org.apache.ignite.internal.marshaller.BinaryMode.INT;
import static org.apache.ignite.internal.marshaller.BinaryMode.LONG;
import static org.apache.ignite.internal.marshaller.BinaryMode.NUMBER;
import static org.apache.ignite.internal.marshaller.BinaryMode.SHORT;
import static org.apache.ignite.internal.marshaller.BinaryMode.STRING;
import static org.apache.ignite.internal.marshaller.BinaryMode.TIME;
import static org.apache.ignite.internal.marshaller.BinaryMode.TIMESTAMP;
import static org.apache.ignite.internal.marshaller.BinaryMode.UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;
import org.apache.ignite.internal.marshaller.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.marshaller.testobjects.TestSimpleObject;
import org.apache.ignite.internal.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Check field accessor correctness.
 */
public class FieldAccessorTest {
    /** Random. */
    private Random rnd;

    /**
     * Init random and print seed before each test.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();

        System.out.println("Using seed: " + seed + "L;");

        rnd = new Random(seed);
    }

    /**
     * FieldAccessor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void fieldAccessor() throws Exception {
        MarshallerColumn[] cols = new MarshallerColumn[]{
                new MarshallerColumn("primitiveByteCol", BYTE),
                new MarshallerColumn("primitiveShortCol", SHORT),
                new MarshallerColumn("primitiveIntCol", INT),
                new MarshallerColumn("primitiveLongCol", LONG),
                new MarshallerColumn("primitiveFloatCol", FLOAT),
                new MarshallerColumn("primitiveDoubleCol", DOUBLE),

                new MarshallerColumn("byteCol", BYTE),
                new MarshallerColumn("shortCol", SHORT),
                new MarshallerColumn("intCol", INT),
                new MarshallerColumn("longCol", LONG),
                new MarshallerColumn("floatCol", FLOAT),
                new MarshallerColumn("doubleCol", DOUBLE),

                new MarshallerColumn("dateCol", DATE),
                new MarshallerColumn("timeCol", TIME),
                new MarshallerColumn("dateTimeCol", DATETIME),
                new MarshallerColumn("timestampCol", TIMESTAMP),

                new MarshallerColumn("uuidCol", UUID),
                new MarshallerColumn("bitmaskCol", BITSET),
                new MarshallerColumn("stringCol", STRING),
                new MarshallerColumn("bytesCol", BYTE_ARR),
                new MarshallerColumn("numberCol", NUMBER),
                new MarshallerColumn("decimalCol", DECIMAL),
        };

        final Pair<MarshallerWriter, MarshallerReader> mocks = createMocks();

        final MarshallerWriter MarshallerWriter = mocks.getFirst();
        final MarshallerReader row = mocks.getSecond();

        final TestObjectWithAllTypes obj = TestObjectWithAllTypes.randomObject(rnd);

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor
                    .create(TestObjectWithAllTypes.class, cols[i].name(), cols[i], i);

            accessor.write(MarshallerWriter, obj);
        }

        final TestObjectWithAllTypes restoredObj = new TestObjectWithAllTypes();

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor
                    .create(TestObjectWithAllTypes.class, cols[i].name(), cols[i], i);

            accessor.read(row, restoredObj);
        }

        assertEquals(obj, restoredObj);
    }

    /**
     * NullableFieldsAccessor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void nullableFieldsAccessor() throws Exception {
        MarshallerColumn[] cols = new MarshallerColumn[]{
                new MarshallerColumn("intCol", INT),
                new MarshallerColumn("longCol", LONG),

                new MarshallerColumn("stringCol", STRING),
                new MarshallerColumn("bytesCol", BYTE_ARR),
        };

        final Pair<MarshallerWriter, MarshallerReader> mocks = createMocks();

        final MarshallerWriter MarshallerWriter = mocks.getFirst();
        final MarshallerReader row = mocks.getSecond();

        final TestSimpleObject obj = TestSimpleObject.randomObject(rnd);

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor
                    .create(TestSimpleObject.class, cols[i].name(), cols[i], i);

            accessor.write(MarshallerWriter, obj);
        }

        final TestSimpleObject restoredObj = new TestSimpleObject();

        for (int i = 0; i < cols.length; i++) {
            FieldAccessor accessor = FieldAccessor
                    .create(TestSimpleObject.class, cols[i].name(), cols[i], i);

            accessor.read(row, restoredObj);
        }

        assertEquals(obj, restoredObj);
    }

    /**
     * IdentityAccessor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @throws Exception If failed.
     */
    @Test
    public void identityAccessor() throws Exception {
        final FieldAccessor accessor = FieldAccessor.createIdentityAccessor(
                "col0",
                0,
                STRING);

        assertEquals("Some string", accessor.value("Some string"));

        final Pair<MarshallerWriter, MarshallerReader> mocks = createMocks();

        accessor.write(mocks.getFirst(), "Other string");
        assertEquals("Other string", accessor.read(mocks.getSecond()));
    }

    /**
     * WrongIdentityAccessor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    @Test
    public void wrongIdentityAccessor() {
        final FieldAccessor accessor = FieldAccessor.createIdentityAccessor(
                "col0",
                42,
                UUID);

        assertEquals("Some string", accessor.value("Some string"));

        final Pair<MarshallerWriter, MarshallerReader> mocks = createMocks();

        assertThrows(
                MarshallerException.class,
                () -> accessor.write(mocks.getFirst(), "Other string"),
                "Failed to write field [id=42]"
        );
    }

    /**
     * Creates mock pair for {@link MarshallerWriter} and {@link MarshallerReader}.
     *
     * @return Pair of mocks.
     */
    private Pair<MarshallerWriter, MarshallerReader> createMocks() {
        final ArrayList<Object> vals = new ArrayList<>();

        final MarshallerWriter mockedAsm = Mockito.mock(MarshallerWriter.class);
        final MarshallerReader mockedRow = Mockito.mock(MarshallerReader.class);

        final Answer<Void> asmAnswer = new Answer<>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                if ("appendNull".equals(invocation.getMethod().getName())) {
                    vals.add(null);
                } else {
                    vals.add(invocation.getArguments()[0]);
                }

                return null;
            }
        };

        final Answer<Object> rowAnswer = new Answer<>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                final int idx = invocation.getArgument(0, Integer.class);

                return vals.get(idx);
            }
        };

        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendNull();
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendByte(Mockito.anyByte());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendShort(Mockito.anyShort());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendInt(Mockito.anyInt());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendLong(Mockito.anyLong());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendFloat(Mockito.anyFloat());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendDouble(Mockito.anyDouble());

        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendUuid(Mockito.any(java.util.UUID.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendBitmask(Mockito.any(BitSet.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendString(Mockito.anyString());
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendBytes(Mockito.any(byte[].class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendNumber(Mockito.any(BigInteger.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendDecimal(Mockito.any(BigDecimal.class));

        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendDate(Mockito.any(LocalDate.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendDateTime(Mockito.any(LocalDateTime.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendTime(Mockito.any(LocalTime.class));
        Mockito.doAnswer(asmAnswer).when(mockedAsm).appendTimestamp(Mockito.any(Instant.class));

        Mockito.doAnswer(rowAnswer).when(mockedRow).byteValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).byteValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).shortValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).shortValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).intValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).intValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).longValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).longValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).floatValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).floatValueBoxed(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).doubleValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).doubleValueBoxed(Mockito.anyInt());

        Mockito.doAnswer(rowAnswer).when(mockedRow).dateValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).timeValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).dateTimeValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).timestampValue(Mockito.anyInt());

        Mockito.doAnswer(rowAnswer).when(mockedRow).uuidValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).bitmaskValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).stringValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).bytesValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).numberValue(Mockito.anyInt());
        Mockito.doAnswer(rowAnswer).when(mockedRow).decimalValue(Mockito.anyInt());

        return new Pair<>(mockedAsm, mockedRow);
    }
}
