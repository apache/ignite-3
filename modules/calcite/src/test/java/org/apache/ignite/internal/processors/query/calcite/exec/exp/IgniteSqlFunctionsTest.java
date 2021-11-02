package org.apache.ignite.internal.processors.query.calcite.exec.exp;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 *
 */
public class IgniteSqlFunctionsTest {
    /** */
    @Test
    public void testBigDecimalToString() {
        assertNull(IgniteSqlFunctions.toString(null));

        assertEquals(
            "10",
            IgniteSqlFunctions.toString(BigDecimal.valueOf(10))
        );

        assertEquals(
            "9223372036854775807",
            IgniteSqlFunctions.toString(BigDecimal.valueOf(Long.MAX_VALUE))
        );

        assertEquals(
            "340282350000000000000000000000000000000",
            IgniteSqlFunctions.toString(new BigDecimal(String.valueOf(Float.MAX_VALUE)))
        );

        assertEquals(
            "-340282346638528860000000000000000000000",
            IgniteSqlFunctions.toString(BigDecimal.valueOf(-Float.MAX_VALUE))
        );
    }

    /** */
    @Test
    public void testBooleanPrimitiveToBigDecimal() {
        assertThrows(
            UnsupportedOperationException.class,
            () -> IgniteSqlFunctions.toBigDecimal(true, 10, 10));
    }

    /** */
    @Test
    public void testBooleanObjectToBigDecimal() {
        assertThrows(
            UnsupportedOperationException.class,
            () -> IgniteSqlFunctions.toBigDecimal(Boolean.valueOf(true), 10, 10));
    }

    /** */
    @Test
    public void testPrimitiveToDecimal() {
        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal((byte)10, 10, 0)
        );

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal((short)10, 10, 0)
        );

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal(10, 10, 0)
        );

        assertEquals(
            new BigDecimal("10.0"),
            IgniteSqlFunctions.toBigDecimal(10L, 10, 1)
        );

        assertEquals(
            new BigDecimal("10.101"),
            IgniteSqlFunctions.toBigDecimal(10.101f, 10, 3)
        );

        assertEquals(
            new BigDecimal("10.101"),
            IgniteSqlFunctions.toBigDecimal(10.101d, 10, 3)
        );
    }

    /** */
    @Test
    public void testObjectToDecimal() {
        assertNull(IgniteSqlFunctions.toBigDecimal((Object)null, 10, 0));

        assertNull(IgniteSqlFunctions.toBigDecimal((Double)null, 10, 0));

        assertNull(IgniteSqlFunctions.toBigDecimal((String)null, 10, 0));

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal(new Byte("10"), 10, 0)
        );

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal(Short.valueOf("10"), 10, 0)
        );

        assertEquals(
            new BigDecimal(10),
            IgniteSqlFunctions.toBigDecimal(Integer.valueOf(10), 10, 0)
        );

        assertEquals(
            new BigDecimal("10.0"),
            IgniteSqlFunctions.toBigDecimal(Long.valueOf(10L), 10, 1)
        );

        assertEquals(
            new BigDecimal("10.101"),
            IgniteSqlFunctions.toBigDecimal(Float.valueOf(10.101f), 10, 3)
        );

        assertEquals(
            new BigDecimal("10.101"),
            IgniteSqlFunctions.toBigDecimal(Double.valueOf(10.101d), 10, 3)
        );
    }
}
