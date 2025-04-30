package org.apache.ignite.internal.configuration;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class DeletedKeysFilter {
    /**
     * Filters out keys that match the deleted prefixes and returns the ignored keys.
     *
     * @param values The map of values to filter.
     * @param deletedPrefixes The collection of deleted key patterns.
     * @return A collection of keys that were ignored.
     */
    static List<String> ignoreDeleted(
            Map<String, ? extends Serializable> values,
            Collection<String> deletedPrefixes
    ) {
        if (deletedPrefixes.isEmpty()) {
            return List.of();
        }

        List<Pattern> patterns = deletedPrefixes.stream()
                .map(deletedKey -> deletedKey.replace(".", "\\.").replace("*", ".*") + ".*")
                .map(Pattern::compile)
                .collect(Collectors.toList());

        List<String> ignoredKeys = values.keySet().stream()
                .filter(serializable -> isDeleted(serializable, patterns))
                .collect(Collectors.toList());

        for (String key : ignoredKeys) {
            values.remove(key);
        }

        return ignoredKeys;
    }

    private static boolean isDeleted(String key, Collection<Pattern> patterns) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(key).find()) {
                return true;
            }
        }

        return false;
    }
}
