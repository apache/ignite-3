package org.apache.ignite.configuration;

import org.apache.ignite.configuration.internal.DynamicConfiguration;

public class PublicConfigurator<T extends ConfigurationTree<?, ?>> {

    private T root;

    public <VIEW, INIT, CHANGE> PublicConfigurator(Configurator<? extends DynamicConfiguration<VIEW, INIT, CHANGE>> configurator) {
        final ConfigurationTree<VIEW, CHANGE> root = configurator.getRoot();
        this.root = (T) root;
    }

    public T getRoot() {
        return root;
    }
}
