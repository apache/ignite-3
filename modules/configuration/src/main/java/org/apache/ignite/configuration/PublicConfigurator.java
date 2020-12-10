package org.apache.ignite.configuration;

import org.apache.ignite.configuration.internal.DynamicConfiguration;

/**
 * Public configurator.
 * @param <T> Public type.
 */
public class PublicConfigurator<T extends ConfigurationTree<?, ?>> {
    /** Configuration root. */
    private T root;

    public <VIEW, INIT, CHANGE> PublicConfigurator(Configurator<? extends DynamicConfiguration<VIEW, INIT, CHANGE>> configurator) {
        final ConfigurationTree<VIEW, CHANGE> root = configurator.getRoot();
        this.root = (T) root;
    }

    /**
     * Get root of the configuration.
     * @return Configuration root.
     */
    public T getRoot() {
        return root;
    }
}
