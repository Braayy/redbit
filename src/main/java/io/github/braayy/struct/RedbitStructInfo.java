package io.github.braayy.struct;

import io.github.braayy.column.RedbitColumnInfo;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RedbitStructInfo {

    private final String name;
    private final RedbitColumnInfo idColumn;
    private final List<RedbitColumnInfo> columns;

    public RedbitStructInfo(String name, RedbitColumnInfo idColumn, List<RedbitColumnInfo> columns) {
        this.name = name;
        this.idColumn = idColumn;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public RedbitColumnInfo getIdColumn() {
        return idColumn;
    }

    public Collection<RedbitColumnInfo> getColumns() {
        return columns;
    }

    public Collection<RedbitColumnInfo> getAllColumns() {
        List<RedbitColumnInfo> allColumns = new ArrayList<>(columns);
        allColumns.add(0, idColumn);

        return allColumns;
    }

    @Nullable
    public RedbitColumnInfo getColumnFromName(String name) {
        for (RedbitColumnInfo columnInfo : columns) {
            if (columnInfo.getName().equals(name)) return columnInfo;
        }

        return null;
    }
}
