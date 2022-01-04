package io.github.braayy.struct;

import io.github.braayy.Redbit;
import io.github.braayy.RedbitQuery;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.synchronization.RedbitSynchronizationEntry.Operation;
import io.github.braayy.utils.RedbitQueryBuilders;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPooled;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;

public class RedbitStruct implements AutoCloseable {

    private RedbitQuery currentQuery;

    public boolean upsert() {
        return upsert(false, false);
    }

    public boolean upsert(boolean ignoreNullValues) {
        return upsert(ignoreNullValues, false);
    }

    private boolean upsert(boolean ignoreNullValues, boolean fromDatabase) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            Map<String, String> valueMap = getStructValues(structInfo, ignoreNullValues);

            String idValue = valueMap.remove(structInfo.getIdColumn().getName());
            if (idValue == null || Objects.equals(idValue, ""))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            jedis.hset(key, valueMap);

            if (!fromDatabase)
                Redbit.getSynchronizer().addModifiedKey(structInfo, idValue, Operation.UPSERT);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public boolean delete() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            Object idValue = getIdFieldValue(structInfo);
            if (idValue == null || Objects.equals(idValue, ""))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            jedis.del(key);

            Redbit.getSynchronizer().addModifiedKey(structInfo, idValue.toString(), Operation.DELETE);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public FetchResult fetch() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            Object idValue = getIdFieldValue(structInfo);
            if (idValue == null || Objects.equals(idValue, ""))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            Map<String, String> valueMap = jedis.hgetAll(key);

            if (!valueMap.isEmpty()) {
                setFieldsValueFromRedis(structInfo, valueMap);

                return FetchResult.FOUND;
            }

            String strQuery = RedbitQueryBuilders.buildSelectQuery(structInfo, idValue.toString());
            try (RedbitQuery query = Redbit.sqlQuery(strQuery)) {
                ResultSet set = query.executeQuery();

                if (set.next()) {
                    setFieldsValueFromResultSet(structInfo, set, false);
                    upsert(false, true);

                    return FetchResult.FOUND;
                }
            }

            return FetchResult.NOT_FOUND;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while fetching struct from redis/mysql", exception);

            return FetchResult.ERROR;
        }
    }

    public boolean customFetch(String customQuery) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String strQuery = prepareCustomQuery(structInfo, customQuery);

            if (Redbit.getConfig().isDebug())
                Redbit.getLogger().info("[SQL] " + strQuery);

            currentQuery = Redbit.sqlQuery(strQuery);
            currentQuery.executeQuery();

            return next();
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while custom fetching struct from redis/mysql", exception);

            return false;
        }
    }

    public boolean next() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            if (currentQuery == null)
                throw new IllegalArgumentException("No current query was set! RedbitStruct#customFetch(String) should do it");

            if (currentQuery.getStatement().isClosed())
                throw new IllegalArgumentException("Current Query's Statement is closed!");

            ResultSet resultSet = currentQuery.getResultSet();

            if (resultSet == null) {
                currentQuery.close();
                currentQuery = null;
                throw new IllegalArgumentException("No ResultSet found in Current Query!");
            }

            if (!resultSet.next()) {
                currentQuery.close();
                currentQuery = null;
                return false;
            }

            setFieldsValueFromResultSet(structInfo, resultSet, true);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while advancing to next entry", exception);

            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        if (currentQuery != null) currentQuery.close();
    }

    private String prepareCustomQuery(RedbitStructInfo structInfo, String customQuery) {
        return customQuery.replace("{table}", structInfo.getName());
    }

    private void setFieldsValueFromResultSet(RedbitStructInfo structInfo, ResultSet set, boolean ignoreNonSelectedColumns) throws SQLException, NoSuchFieldException, IllegalAccessException {
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

    private void setFieldsValueFromRedis(RedbitStructInfo structInfo, Map<String, String> valueMap) throws NoSuchFieldException, IllegalAccessException {
        for (RedbitColumnInfo columnInfo : structInfo.getColumns()) {
            Field field = getClass().getDeclaredField(columnInfo.getFieldName());
            field.setAccessible(true);

            Class<?> type = field.getType();

            String value = valueMap.get(columnInfo.getName());

            if (Objects.equals(value, "") && columnInfo.isNullable()) {
                field.set(this, null);
                continue;
            }

            if (type.equals(Byte.class))
                field.set(this, Byte.parseByte(value));
            else if (type.equals(Character.class))
                field.set(this, value.charAt(0));
            else if (type.equals(Short.class))
                field.set(this, Short.parseShort(value));
            else if (type.equals(Integer.class))
                field.set(this, Integer.parseInt(value));
            else if (type.equals(Long.class))
                field.set(this, Long.parseLong(value));
            else if (type.equals(String.class))
                field.set(this, value);
            else
                throw new IllegalArgumentException(type.getName() + " type is not supported by redbit");
        }
    }

    private Map<String, String> getStructValues(RedbitStructInfo structInfo, boolean ignoreNullValues) throws NoSuchFieldException, IllegalAccessException {
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

    private Object getIdFieldValue(RedbitStructInfo structInfo) throws NoSuchFieldException, IllegalAccessException {
        RedbitColumnInfo columnInfo = structInfo.getIdColumn();
        Field field = getClass().getDeclaredField(columnInfo.getFieldName());
        field.setAccessible(true);

        return field.get(this);
    }

}
