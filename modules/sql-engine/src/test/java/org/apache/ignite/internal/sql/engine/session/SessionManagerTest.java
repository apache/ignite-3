package org.apache.ignite.internal.sql.engine.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.apache.ignite.internal.sql.engine.property.Property;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SessionManagerTest {

    private SessionManager sessionMgr;
    private Map<SessionId, Session> activeSessions;

    @BeforeEach
    void beforeEach() {
        sessionMgr = new SessionManager("test", System::currentTimeMillis);
        activeSessions = IgniteTestUtils.getFieldValue(sessionMgr, "activeSessions");
    }

    @AfterEach
    void afterEach() throws Exception {
        sessionMgr.stop();
    }

    @Test
    void createSession() {
        assertEquals(0, activeSessions.size());

        SessionId sessionId1 = sessionMgr.createSession(1000, null);
        assertNotNull(sessionId1);

        SessionId sessionId2 = sessionMgr.createSession(1000, null);
        assertNotNull(sessionId2);

        assertNotEquals(sessionId1, sessionId2);
        assertEquals(2, activeSessions.size());
    }

    @Test
    void sessionGet() {
        PropertiesHolder propHldr = createPropertyHolder();

        SessionId sessionId = sessionMgr.createSession(12345, propHldr);

        Session session = sessionMgr.session(sessionId);
        assertNotNull(session);
        assertSame(propHldr, session.queryProperties());
        assertEquals(12345, session.getIdleTimeoutMs());

        SessionId unknownSessionId = new SessionId(UUID.randomUUID());
        assertNull(sessionMgr.session(unknownSessionId));
    }

    @Test
    void touchSessionDuringGet() throws InterruptedException {
        long idleTimeout = 20;

        SessionId sessionId = sessionMgr.createSession(idleTimeout, null);

        long time = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            sessionMgr.session(sessionId);
            Thread.sleep(5);
        }
        assertTrue(System.currentTimeMillis() - time > idleTimeout);

        Session session = sessionMgr.session(sessionId);
        assertFalse(session.expired());

        Thread.sleep(idleTimeout + 10);
        assertTrue(session.expired());
        assertEquals(1, activeSessions.size());

        // touch session don't change already expire state.
        sessionMgr.session(sessionId);
        assertTrue(session.expired());
        assertEquals(1, activeSessions.size());

    }

    @Test
    void sessionExpiration() throws InterruptedException {
        IgniteTestUtils.setFieldValue(sessionMgr, "checkPeriod", 100L);
        sessionMgr.start();

        SessionId sessionId = sessionMgr.createSession(10, null);

        Session session = sessionMgr.session(sessionId);
        assertFalse((session.expired()));
        assertEquals(1, activeSessions.size());

        Thread.sleep(150);
        assertEquals(0, activeSessions.size());
        assertNull(sessionMgr.session(sessionId));
    }


    private PropertiesHolder createPropertyHolder() {
        return new PropertiesHolder() {
            @Override
            public <T> @Nullable T get(Property<T> prop) {
                return null;
            }

            @Override
            public <T> @Nullable T getOrDefault(Property<T> prop, @Nullable T defaultValue) {
                return null;
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public Map<Property<?>, Object> toMap() {
                return null;
            }
        };
    }
}
