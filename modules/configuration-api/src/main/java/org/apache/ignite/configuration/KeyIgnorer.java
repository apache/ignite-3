package org.apache.ignite.configuration;

import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Determines if key should be ignored while processing configuration. */
@FunctionalInterface
public interface KeyIgnorer {
    boolean shouldIgnore(String key);

    /** Creates a key ignorer that uses specified deletedPrefixes. */
    static KeyIgnorer fromDeletedPrefixes(Collection<String> deletedPrefixes) {
        Collection<Pattern> patterns = deletedPrefixes.stream()
                .map(deletedKey -> deletedKey.replace(".", "\\.").replace("*", "[^.]*") + "(\\..*)?")
                .map(Pattern::compile)
                .collect(Collectors.toList());

        return key -> patterns.stream().anyMatch(pattern -> pattern.matcher(key).matches());
    }
}
