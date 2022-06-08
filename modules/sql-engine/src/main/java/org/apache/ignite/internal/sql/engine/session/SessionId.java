package org.apache.ignite.internal.sql.engine.session;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.tostring.S;

public class SessionId {
    private final UUID id;

    public SessionId(UUID id) {
        this.id = Objects.requireNonNull(id, "id");
    }

    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SessionId sessionId = (SessionId) o;

        return id.equals(sessionId.id);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
