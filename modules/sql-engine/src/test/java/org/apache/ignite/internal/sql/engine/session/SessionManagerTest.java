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

package org.apache.ignite.internal.sql.engine.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.sql.engine.property.PropertiesHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * SessionManager tests.
 */
class SessionManagerTest {

    private SessionManager sessionMgr;
    private AtomicLong clock = new AtomicLong(System.currentTimeMillis());

    @BeforeEach
    void beforeEach() {
        sessionMgr = new SessionManager("test", 20, () -> clock.get());
    }

    @AfterEach
    void afterEach() {
        sessionMgr.stop();
    }

    @Test
    void sessionGet() {
        PropertiesHolder propHldr = PropertiesHolder.holderFor(Map.of());

        SessionId sessionId = sessionMgr.createSession(12345, propHldr);

        Session session = sessionMgr.session(sessionId);
        assertNotNull(session);
        assertSame(propHldr, session.queryProperties());
        assertEquals(12345, session.getIdleTimeoutMs());

        SessionId unknownSessionId = new SessionId(UUID.randomUUID());
        assertNull(sessionMgr.session(unknownSessionId));
    }

    @Test
    void expirationThreadTests() throws InterruptedException {
        long idleTimeout = 20;

        SessionId sessionId = sessionMgr.createSession(idleTimeout, null);

        long time = System.currentTimeMillis();
        clock.set(time);

        for (int i = 0; i < 10; i++) {
            Session session = sessionMgr.session(sessionId);
            Thread.sleep(5);
            clock.set(System.currentTimeMillis());
        }
        assertTrue(System.currentTimeMillis() - time > idleTimeout);

        Session session = sessionMgr.session(sessionId);
        assertFalse(session.expired());
        clock.addAndGet(idleTimeout + 1);
        assertTrue(session.expired());

        assertNull(sessionMgr.session(sessionId));
    }

    @Test
    void sessionExpiration() throws InterruptedException {
        clock.set(1);
        SessionId sessionId = sessionMgr.createSession(2, null);

        Session session = sessionMgr.session(sessionId);
        assertFalse(session.expired());

        //period is small to expire session
        clock.set(2);
        assertFalse(session.expired());

        //period is enough to session expired, but we touch session and prolong times live
        clock.set(4);
        assertNotNull(sessionMgr.session(sessionId));
        assertFalse(session.expired());

        clock.set(7);
        assertTrue(session.expired());
        assertNull(sessionMgr.session(sessionId));
        // touch session don't change already expire state.
        assertTrue(session.expired());

    }

}
