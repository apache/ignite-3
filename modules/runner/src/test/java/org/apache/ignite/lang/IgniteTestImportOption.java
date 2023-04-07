package org.apache.ignite.lang;

import static com.tngtech.archunit.core.importer.ImportOption.Predefined.ONLY_INCLUDE_TESTS;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.Location;
import java.util.regex.Pattern;

public class IgniteTestImportOption implements ImportOption {
    private final Pattern integrationTestPattern = Pattern.compile(".*/build/classes/([^/]+/)integrationTest/.*");
    @Override
    public boolean includes(Location location) {
        return ONLY_INCLUDE_TESTS.includes(location) || location.matches(integrationTestPattern);
    }
}
