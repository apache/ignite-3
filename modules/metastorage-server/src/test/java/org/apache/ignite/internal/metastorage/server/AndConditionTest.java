package org.apache.ignite.internal.metastorage.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.ignite.internal.util.ArrayUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AndConditionTest {

    private Condition cond1;
    private Condition cond2;
    private Condition cond3;
    private Condition cond4;

    private final Entry[] entries = new Entry[] {
            new Entry(new byte[]{1}, new byte[]{10}, 1, 1),
            new Entry(new byte[]{2}, new byte[]{20}, 2, 3),
            new Entry(new byte[]{3}, new byte[]{30}, 3, 4),

    };

    @BeforeEach
    public void setUp() {
        cond1 = cond(new byte[][] {{1}, {2}}, true);
        cond2 = cond(new byte[][] {{3}}, true);
        cond3 = cond(new byte[][] {{5}, {6}}, false);
        cond4 = cond(new byte[][] {{7}}, false);
    }

    @Test
    public void testTrueTrue() {
        var cond = new AndCondition(cond1, cond2);

        assertArrayEquals(ArrayUtils.concat(cond1.keys(), cond2.keys()), cond.keys());
        assertTrue(cond.test(entries));
        verify(cond1, times(1)).test(Arrays.copyOf(entries, 2));
        verify(cond2, times(1)).test(Arrays.copyOfRange(entries, 2, 3));
    }

    @Test
    public void testTrueFalse() {
        var cond = new AndCondition(cond2, cond3);

        assertArrayEquals(ArrayUtils.concat(cond2.keys(), cond3.keys()), cond.keys());
        assertFalse(cond.test(entries));
        verify(cond2, times(1)).test(Arrays.copyOf(entries, 1));
        verify(cond3, times(1)).test(Arrays.copyOfRange(entries, 1, 3));
    }

    @Test
    public void testFalseTrue() {
        var cond = new AndCondition(cond3, cond2);

        assertArrayEquals(ArrayUtils.concat(cond3.keys(), cond2.keys()), cond.keys());
        assertFalse(cond.test(entries));
        verify(cond3, times(1)).test(Arrays.copyOf(entries, 2));
        verify(cond2, times(1)).test(Arrays.copyOfRange(entries, 2, 3));
    }

    @Test
    public void testFalseFalse() {
        var cond = new AndCondition(cond3, cond4);

        assertArrayEquals(ArrayUtils.concat(cond3.keys(), cond4.keys()), cond.keys());
        assertFalse(cond.test(entries));
        verify(cond3, times(1)).test(Arrays.copyOf(entries, 2));
        verify(cond4, times(1)).test(Arrays.copyOfRange(entries, 2, 3));
    }

    private static Condition cond(byte[][] keys, boolean result) {
        var m = mock(Condition.class);

        when(m.keys()).thenReturn(keys);

        when(m.test(any())).thenReturn(result);

        return m;
    }
}
