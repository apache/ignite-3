package org.apache.ignite.configuration;

import org.apache.ignite.configuration.validation.ConfigurationValidationException;

/**
 * Base interface for configuration.
 * @param <VALUE> Type of the value.
 * @param <CHANGE> Type of the object that changes the value of configuration.
 */
public interface ConfigurationProperty<VALUE, CHANGE> {
    /**
     * Get key of this node.
     * @return Key.
     */
    String key();

    /**
     * Get value of this property.
     * @return Value of this property.
     */
    VALUE value();

    /**
     * Change this configuration node value.
     * @param change CHANGE object.
     * @throws ConfigurationValidationException If validation failed.
     */
    void change(CHANGE change) throws ConfigurationValidationException;
}
