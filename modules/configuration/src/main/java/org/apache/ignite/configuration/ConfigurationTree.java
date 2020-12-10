package org.apache.ignite.configuration;

import java.util.Map;

public interface ConfigurationTree<VIEW, CHANGE> extends ConfigurationProperty<VIEW, CHANGE> {

    Map<String, ConfigurationProperty<?, ?>> members();

}
