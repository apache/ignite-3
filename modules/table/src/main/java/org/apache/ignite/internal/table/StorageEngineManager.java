package org.apache.ignite.internal.table;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;

/**
 * TBD.
 */
public interface StorageEngineManager {
    String ROCKS_DB = "ROCKS_DB";

    /**
     * TBD.
     *
     * @param engineName TBD.
     * @return TBD.
     */
    StorageEngineBridge get(String engineName);

    /**
     * TBD.
     *
     * @param name   TBD.
     * @param bridge TBD.
     */
    void register(String name, StorageEngineBridge bridge);

    /**
     * TBD.
     */
    class StorageEngineManagerImpl implements StorageEngineManager {
        private final Map<String, StorageEngineBridge> bridges = new ConcurrentHashMap<>();

        @Override
        @NotNull
        public StorageEngineBridge get(String engineName) {
            StorageEngineBridge bridge = bridges.get(engineName);

            if (bridge == null) {
                throw new IllegalArgumentException("There is no storage engine with name " + engineName);
            }

            return bridges.get(ROCKS_DB);
        }

        @Override
        public void register(String name, StorageEngineBridge bridge) {
            if (bridges.putIfAbsent(name, bridge) != null) {
                throw new IllegalArgumentException("A storage engine with name " + name + " already registered");
            }
        }
    }
}
