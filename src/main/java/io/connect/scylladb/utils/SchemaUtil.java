package io.connect.scylladb.utils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.config.ConfigException;

public class SchemaUtil {

    private static final List<String> DIRECTION_LIST = Arrays.asList("desc", "asc");

    public static void validateClusterKeyDirections(final Map<String, String> directionsMap) {
        if (Objects.nonNull(directionsMap)) {
            directionsMap
                .values()
                .forEach(direction -> {
                    if (!DIRECTION_LIST.contains(direction)) {
                        throw new ConfigException(String.format("Cluster key (%s) direction is not valid.", direction));
                    }
                });
        }
    }

    public static Map<String, String> parseMappings(final List<String> properties) {
        final LinkedHashMap<String, String> map = new LinkedHashMap<>();

        for (final String property : properties) {
            final String[] parts = property.split(":");

            if (parts.length != 2) {
                throw new ConfigException(String.format("The properties (%s) is not valid to parse", property));
            }

            map.put(parts[0], parts[1]);
        }

        return map;
    }
}
