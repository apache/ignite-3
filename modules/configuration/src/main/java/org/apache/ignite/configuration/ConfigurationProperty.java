package org.apache.ignite.configuration;

import org.apache.ignite.configuration.validation.ConfigurationValidationException;

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
