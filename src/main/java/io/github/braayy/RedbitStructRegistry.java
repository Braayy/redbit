package io.github.braayy;

import io.github.braayy.column.RedbitColumn;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.struct.RedbitStruct;
import io.github.braayy.struct.RedbitStructInfo;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Field;
import java.util.*;

public class RedbitStructRegistry {

    RedbitStructRegistry() {
        this.structMap = new HashMap<>();
    }

    private final Map<Class<? extends RedbitStruct>, RedbitStructInfo> structMap;

    public <T extends RedbitStruct> void registerStruct(String structName, Class<T> structClass) {
        RedbitColumnInfo structIdColumn = null;
        List<RedbitColumnInfo> columns = new ArrayList<>();

        for (Field field : structClass.getDeclaredFields()) {
            RedbitColumn redbitColumn = field.getAnnotation(RedbitColumn.class);
            if (redbitColumn == null) continue;

            String fieldName = field.getName();
            String name = redbitColumn.name();
            String sqlType = redbitColumn.sqlType();
            String defaultValue = redbitColumn.defaultValue();
            int length = redbitColumn.length();
            boolean idColumn = redbitColumn.idColumn();
            boolean autoIncrement = redbitColumn.autoIncrement();
            boolean nullable = redbitColumn.nullable();

            if (idColumn && nullable)
                throw new IllegalArgumentException("Id column cannot be nullable in struct " + structName);

            if (autoIncrement && !idColumn)
                throw new IllegalArgumentException("Only a id column can have a auto increment in struct " + structName);

            if (field.getType().isPrimitive())
                throw new IllegalArgumentException("Use boxed version of primitive types for nullability in struct " + structName);

            if (Objects.equals(name, ""))
                name = fieldName;

            StringBuilder sqlCreation = new StringBuilder(sqlType);
            if (length > 0)
                sqlCreation.append('(').append(length).append(')');
            if (!nullable)
                sqlCreation.append(" NOT NULL");
            if (autoIncrement)
                sqlCreation.append(" AUTO_INCREMENT");
            if (!Objects.equals(defaultValue, ""))
                sqlCreation.append(" DEFAULT '").append(defaultValue).append('\'');

            RedbitColumnInfo columnInfo = new RedbitColumnInfo(fieldName, name, sqlCreation.toString(), defaultValue, idColumn, autoIncrement, nullable);

            if (idColumn) {
                if (structIdColumn != null)
                    throw new IllegalArgumentException("Two or more id columns detected in struct " + structName);

                structIdColumn = columnInfo;

                continue;
            }

            columns.add(columnInfo);
        }

        if (structIdColumn == null)
            throw new IllegalArgumentException("No id column found in struct " + structName);

        RedbitStructInfo structInfo = new RedbitStructInfo(structName, structIdColumn, columns);

        structMap.put(structClass, structInfo);
    }

    @Nullable
    public RedbitStructInfo getStructInfo(Class<? extends RedbitStruct> tableClass) {
        return structMap.get(tableClass);
    }

    public Collection<RedbitStructInfo> getStructs() {
        return structMap.values();
    }
}
