package io.github.braayy.utils;

import java.util.Objects;

public class RedbitUtils {

    public static String[] toStringArray(Object[] input) {
        if (input.length == 0) return new String[0];

        String[] array = new String[input.length];
        for (int i = 0; i < input.length; i++) {
            Object element = input[i];
            array[i] = element != null ? element.toString() : "";
        }
        return array;
    }

    public static String escapeToSql(String value) {
        return value.replace("'", "''").replace("\\", "\\\\");
    }

    public static boolean isNullString(String value) {
        return value == null || Objects.equals(value, "");
    }

}
