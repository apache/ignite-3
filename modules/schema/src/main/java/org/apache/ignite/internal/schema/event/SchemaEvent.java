package org.apache.ignite.internal.schema.event;

import org.apache.ignite.internal.manager.Event;

/**
 * Schema management events.
 */
public enum SchemaEvent implements Event {
    /** This event is fired when a schema was initialized. */
    INITIALIZED,

    /** This event is fired when a schema was dropped. */
    DROPPED
}

