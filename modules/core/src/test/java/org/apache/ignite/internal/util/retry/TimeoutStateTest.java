package org.apache.ignite.internal.util.retry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TimeoutStateTest {

    private static final int TIMEOUT = 20;
    private static final int ATTEMPT = 10;

    private TimeoutState state;

    @BeforeEach
    void setUp() {
        state = new TimeoutState(TIMEOUT, ATTEMPT);
    }

    @Test
    void testInitialState() {
        assertEquals(TIMEOUT, state.getTimeout());
        assertEquals(ATTEMPT, state.getAttempt());
    }

    @Test
    void testUpdate() {
        int newTimeout = 40;
        int newAttempt = 11;

        long raw = state.getRawState();
        boolean updated = state.update(raw, newTimeout, newAttempt);

        assertTrue(updated);
        assertEquals(newTimeout, state.getTimeout());
        assertEquals(newAttempt, state.getAttempt());
    }

    @Test
    void testUpdateFailsOnStaleSnapshot() {
        long staleSnapshot = state.getRawState();

        // advance state so staleSnapshot is no longer current
        state.update(staleSnapshot, 40, 11);

        // update with stale snapshot must fail
        boolean updated = state.update(staleSnapshot, 80, 12);

        assertFalse(updated);
        // state must remain at the first update's values
        assertEquals(40, state.getTimeout());
        assertEquals(11, state.getAttempt());
    }

    @Test
    void testGetTimeoutAndGetAttemptAreConsistentWithRawState() {
        long raw = state.getRawState();

        assertEquals(state.getTimeout(), TimeoutState.timeout(raw));
        assertEquals(state.getAttempt(), TimeoutState.attempt(raw));
    }

    @Test
    void testPackUnpackRoundtrip() {
        long packed = TimeoutState.pack(TIMEOUT, ATTEMPT);

        assertEquals(TIMEOUT, TimeoutState.timeout(packed));
        assertEquals(ATTEMPT, TimeoutState.attempt(packed));
    }
}
