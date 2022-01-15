package io.github.braayy.utils;

import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.struct.RedbitStruct;
import io.github.braayy.struct.RedbitStructInfo;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RedbitUtils {

    public static String escapeToSql(String value) {
        return value.replace("'", "''").replace("\\", "\\\\");
    }

    public static boolean isNullString(String value) {
        return value == null || Objects.equals(value, "");
    }

    public static void setFieldsValueFromResultSet(RedbitStructInfo structInfo, RedbitStruct struct, ResultSet set, boolean ignoreNonSelectedColumns) throws SQLException, NoSuchFieldException, IllegalAccessException {
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            Field field = struct.getClass().getDeclaredField(columnInfo.getFieldName());
            field.setAccessible(true);

            Class<?> type = field.getType();

            if (ignoreNonSelectedColumns) {
                try {
                    set.findColumn(columnInfo.getName());
                } catch (SQLException exception) {
                    continue;
                }
            }

            if (set.getObject(columnInfo.getName()) == null && columnInfo.isNullable()) {
                field.set(struct, null);
                continue;
            }

            if (type.equals(Byte.class))
                field.set(struct, set.getByte(columnInfo.getName()));
            else if (type.equals(Character.class))
                field.set(struct, set.getString(columnInfo.getName()).charAt(0));
            else if (type.equals(Short.class))
                field.set(struct, set.getShort(columnInfo.getName()));
            else if (type.equals(Integer.class))
                field.set(struct, set.getInt(columnInfo.getName()));
            else if (type.equals(Long.class))
                field.set(struct, set.getLong(columnInfo.getName()));
            else if (type.equals(String.class))
                field.set(struct, set.getString(columnInfo.getName()));
            else
                throw new IllegalArgumentException(type.getName() + " type is not supported by redbit");
        }
    }

    public static void setFieldsValueFromRedis(RedbitStructInfo structInfo, RedbitStruct struct, Map<String, String> valueMap) throws NoSuchFieldException, IllegalAccessException {
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            Field field = struct.getClass().getDeclaredField(columnInfo.getFieldName());
            field.setAccessible(true);

            Class<?> type = field.getType();

            String value = valueMap.get(columnInfo.getName());

            if (isNullString(value) && columnInfo.isNullable()) {
                field.set(struct, null);
                continue;
            }

            if (type.equals(Byte.class))
                field.set(struct, Byte.parseByte(value));
            else if (type.equals(Character.class))
                field.set(struct, value.charAt(0));
            else if (type.equals(Short.class))
                field.set(struct, Short.parseShort(value));
            else if (type.equals(Integer.class))
                field.set(struct, Integer.parseInt(value));
            else if (type.equals(Long.class))
                field.set(struct, Long.parseLong(value));
            else if (type.equals(String.class))
                field.set(struct, value);
            else
                throw new IllegalArgumentException(type.getName() + " type is not supported by redbit");
        }
    }

    public static Map<String, String> getStructValues(RedbitStructInfo structInfo, RedbitStruct struct, boolean ignoreNullValues) throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> valueMap = new HashMap<>();
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            Field field = struct.getClass().getDeclaredField(columnInfo.getFieldName());
            field.setAccessible(true);
            Object value = field.get(struct);
            String strValue = value != null ? value.toString() : null;

            if (isNullString(strValue)) {
                if (ignoreNullValues) continue;

                strValue = columnInfo.getDefaultValue();

                if (isNullString(strValue) && !columnInfo.isNullable())
                    throw new IllegalArgumentException(columnInfo.getName() + " value is null but it is not nullable");
            }

            valueMap.put(columnInfo.getName(), strValue);
        }

        return valueMap;
    }

}
