package org.apache.ignite.lang;

import com.tngtech.archunit.core.importer.Location;
import java.nio.file.Path;
import java.util.Set;

public class LocationProvider {

    public static class RootLocationProvider implements com.tngtech.archunit.junit.LocationProvider {
        @Override
        public Set<Location> get(Class<?> testClass) {
            // ignite-3/modules
            Path modulesRoot = Path.of("").toAbsolutePath().getParent();

            return Set.of(Location.of(modulesRoot));
        }
    }
}
