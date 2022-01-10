package io.github.braayy.struct;

import io.github.braayy.Redbit;
import io.github.braayy.RedbitQuery;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.utils.RedbitQueryBuilders;
import io.github.braayy.utils.RedbitUtils;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;

public class RedbitStruct implements AutoCloseable {

    private RedbitQuery currentQuery;

    public boolean upsertAll() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            Map<String, String> valueMap = getStructValues(structInfo, false);
            String idValue = valueMap.remove(structInfo.getIdColumn().getName());
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            String strQuery = RedbitQueryBuilders.buildUpsertQuery(structInfo, valueMap, false);
            try (RedbitQuery query = Redbit.sqlQuery(strQuery)) {
                query.executeUpdate();
            }

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public boolean deleteById() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String idValue = getIdFieldValue(structInfo);
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            RedbitColumnInfo idColumn = structInfo.getIdColumn();

            String whereClause = "`" + idColumn.getName() + "`='" + RedbitUtils.escapeToSql(idValue) + '\'';

            return deleteWhere(whereClause);
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public boolean deleteAll() {
        return deleteWhere(null);
    }

    public boolean deleteWhere(String whereClause) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String strQuery = RedbitQueryBuilders.buildDeleteQuery(structInfo, whereClause);
            try (RedbitQuery query = Redbit.sqlQuery(strQuery)) {
                query.executeUpdate();
            }

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public FetchResult fetchById() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String idValue = getIdFieldValue(structInfo);
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());
            RedbitColumnInfo idColumn = structInfo.getIdColumn();

            String whereClause = "`" + idColumn.getName() + "`='" + RedbitUtils.escapeToSql(idValue) + "' LIMIT 1";

            return fetchWhere(whereClause);
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return FetchResult.ERROR;
        }
    }

    public FetchResult fetchAll() {
        return fetchWhere(null);
    }

    public FetchResult fetchWhere(String whereClause) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String strQuery = RedbitQueryBuilders.buildSelectByCustomQuery(structInfo, whereClause);

            this.currentQuery = Redbit.sqlQuery(strQuery);
            this.currentQuery.executeQuery();

            return next();
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return FetchResult.ERROR;
        }
    }

    public FetchResult fetchCustom(String customQuery) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String strQuery = prepareCustomQuery(structInfo, customQuery);

            if (Redbit.getConfig().isDebug())
                Redbit.getLogger().info("[SQL] " + strQuery);

            this.currentQuery = Redbit.sqlQuery(strQuery);
            this.currentQuery.executeQuery();

            return next();
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return FetchResult.ERROR;
        }
    }

    public FetchResult next() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            if (this.currentQuery == null)
                throw new IllegalArgumentException("No current query was set! RedbitStruct#customFetch(String) should do it");

            if (this.currentQuery.getStatement().isClosed())
                throw new IllegalArgumentException("Current Query's Statement is closed!");

            ResultSet resultSet = this.currentQuery.getResultSet();

            if (resultSet == null) {
                this.currentQuery.close();
                this.currentQuery = null;
                throw new IllegalArgumentException("Current Query is not a select!");
            }

            if (!resultSet.next()) {
                this.currentQuery.close();
                this.currentQuery = null;
                return FetchResult.NOT_FOUND;
            }

            setFieldsValueFromResultSet(structInfo, resultSet, true);

            return FetchResult.FOUND;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return FetchResult.ERROR;
        }
    }

    @Override
    public void close() throws SQLException {
        if (this.currentQuery != null) this.currentQuery.close();
    }

    private String prepareCustomQuery(RedbitStructInfo structInfo, String customQuery) {
        return customQuery.replace("{table}", structInfo.getName());
    }

    protected void setFieldsValueFromResultSet(RedbitStructInfo structInfo, ResultSet set, boolean ignoreNonSelectedColumns) throws SQLException, NoSuchFieldException, IllegalAccessException {
        for (RedbitColumnInfo columnInfo : structInfo.getColumns()) {
            Field field = getClass().getDeclaredField(columnInfo.getFieldName());
            field.setAccessible(true);

            Class<?> type = field.getType();

            try {
                if (set.getObject(columnInfo.getName()) == null && columnInfo.isNullable()) {
                    field.set(this, null);
                    continue;
                }
            } catch (SQLException exception) {
                if (!ignoreNonSelectedColumns) throw exception;

                String lower = exception.getMessage().toLowerCase(Locale.ROOT);
                if (lower.contains("column") && lower.contains("not") && lower.contains("found"))
                    continue;
                else
                    throw exception;
            }

            if (type.equals(Byte.class))
                field.set(this, set.getByte(columnInfo.getName()));
            else if (type.equals(Character.class))
                field.set(this, set.getString(columnInfo.getName()).charAt(0));
            else if (type.equals(Short.class))
                field.set(this, set.getShort(columnInfo.getName()));
            else if (type.equals(Integer.class))
                field.set(this, set.getInt(columnInfo.getName()));
            else if (type.equals(Long.class))
                field.set(this, set.getLong(columnInfo.getName()));
            else if (type.equals(String.class))
                field.set(this, set.getString(columnInfo.getName()));
            else
                throw new IllegalArgumentException(type.getName() + " type is not supported by redbit");
        }
    }

    protected Map<String, String> getStructValues(RedbitStructInfo structInfo, boolean ignoreNullValues) throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> valueMap = new HashMap<>();
        for (RedbitColumnInfo columnInfo : structInfo.getAllColumns()) {
            Field field = getClass().getDeclaredField(columnInfo.getFieldName());
            field.setAccessible(true);
            Object value = field.get(this);

            if (value == null && ignoreNullValues) continue;

            if (value == null)
                value = columnInfo.getDefaultValue();

            valueMap.put(columnInfo.getName(), value.toString());
        }

        return valueMap;
    }

    protected String getIdFieldValue(RedbitStructInfo structInfo) throws NoSuchFieldException, IllegalAccessException {
        RedbitColumnInfo columnInfo = structInfo.getIdColumn();
        Field field = getClass().getDeclaredField(columnInfo.getFieldName());
        field.setAccessible(true);

        Object value = field.get(this);
        return value != null ? value.toString() : null;
    }
}
