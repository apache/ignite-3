package org.apache.ignite.internal.metastorage.client;

public interface Condition {
    
    Condition and(Condition condition);
    
    Condition or(Condition condition);
}
