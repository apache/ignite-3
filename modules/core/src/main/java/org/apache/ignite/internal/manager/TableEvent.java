package org.apache.ignite.internal.manager;

/**
 * Table managment events.
 */
public enum TableEvent implements Event {
    /** The event happend when a table was created. */
    CREATE,

    /** The event happend when a table was dropped. */
    DROP
}
