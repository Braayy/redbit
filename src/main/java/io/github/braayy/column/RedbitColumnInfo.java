package io.github.braayy.column;

public class RedbitColumnInfo {

    private final String fieldName, name, sqlCreation, defaultValue;
    private final boolean idColumn, autoIncrement;

    public RedbitColumnInfo(String fieldName, String name, String sqlCreation, String defaultValue, boolean idColumn, boolean autoIncrement) {
        this.fieldName = fieldName;
        this.name = name;
        this.sqlCreation = sqlCreation;
        this.defaultValue = defaultValue;
        this.idColumn = idColumn;
        this.autoIncrement = autoIncrement;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getName() {
        return name;
    }

    public String getSqlCreation() {
        return sqlCreation;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isIdColumn() {
        return idColumn;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }
}
