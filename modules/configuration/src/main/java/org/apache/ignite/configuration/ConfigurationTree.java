package org.apache.ignite.configuration;

import java.util.Map;

/**
 * Configuration tree with configuration values and other configuration trees as child nodes.
 * @param <VALUE> Value type of the node.
 * @param <CHANGE> Type of the object that changes this node's value.
 */
public interface ConfigurationTree<VALUE, CHANGE> extends ConfigurationProperty<VALUE, CHANGE> {
    /** Children of the tree. */
    Map<String, ConfigurationProperty<?, ?>> members();

}
