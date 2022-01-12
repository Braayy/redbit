package io.github.braayy.struct;

import io.github.braayy.Redbit;
import io.github.braayy.RedbitQuery;
import io.github.braayy.column.RedbitColumnInfo;
import io.github.braayy.synchronization.RedbitSynchronizationEntry.Operation;
import io.github.braayy.utils.RedbitQueryBuilders;
import io.github.braayy.utils.RedbitUtils;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.util.*;
import java.util.logging.Level;

public class RedbitVolatileStruct extends RedbitStruct {

    private RedbitFetcher currentFetcher;

    @Override
    public boolean upsertAll() {
        return upsertAll(true);
    }

    public boolean upsertAll(boolean synchronize) {
        return upsert(false, synchronize);
    }

    public boolean upsertSome() {
        return upsertSome(true);
    }

    public boolean upsertSome(boolean synchronize) {
        return upsert(true, synchronize);
    }

    private boolean upsert(boolean ignoreNullValues, boolean synchronize) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            if (synchronize && Redbit.getSynchronizer().isShuttingDown())
                throw new IllegalStateException("Synchronized operation ran while synchronizer is shutting down");

            Map<String, String> valueMap = getStructValues(structInfo, ignoreNullValues);

            String idValue = valueMap.remove(structInfo.getIdColumn().getName());
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            jedis.hset(key, valueMap);

            if (synchronize)
                Redbit.getSynchronizer().addModifiedKey(structInfo, idValue, Operation.UPSERT);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @Override
    public boolean deleteById() {
        return deleteById(true);
    }

    public boolean deleteById(boolean synchronize) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            if (synchronize && Redbit.getSynchronizer().isShuttingDown())
                throw new IllegalStateException("Synchronized operation ran while synchronizer is shutting down");

            String idValue = getIdFieldValue(structInfo);
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            jedis.del(key);

            if (synchronize)
                Redbit.getSynchronizer().addModifiedKey(structInfo, idValue, Operation.DELETE);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @Override
    public boolean deleteAll() {
        return deleteAll(true);
    }

    public boolean deleteAll(boolean synchronize) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            if (synchronize && Redbit.getSynchronizer().isShuttingDown())
                throw new IllegalStateException("Synchronized operation ran while synchronizer is shutting down");

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            ScanParams scanParams = new ScanParams().match(structInfo.getName() + ":*").count(10);
            this.currentFetcher = new RedbitFetcher(scanParams);

            String nextKey;
            while ((nextKey = this.currentFetcher.next()) != null) {
                jedis.del(nextKey);
            }

            if (synchronize)
                Redbit.getSynchronizer().addModifiedKey(structInfo, null, Operation.DELETE_ALL);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @Override
    public boolean deleteWhere(String whereClause) {
        throw new UnsupportedOperationException("deleteWhere is not supported by RedbitVolatileStruct");
    }

    @Override
    public FetchResult fetchById() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            String idValue = getIdFieldValue(structInfo);
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);
            Map<String, String> valueMap = jedis.hgetAll(key);

            if (!valueMap.isEmpty()) {
                setFieldsValueFromRedis(structInfo, valueMap);

                return FetchResult.FOUND;
            }

            String strQuery = RedbitQueryBuilders.buildSelectByIdQuery(structInfo, idValue);
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
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return FetchResult.ERROR;
        }
    }

    @Override
    public FetchResult fetchAll() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            ScanParams scanParams = new ScanParams().match(structInfo.getName() + ":*").count(10);
            this.currentFetcher = new RedbitFetcher(scanParams);

            return this.nextInRedis();
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return FetchResult.ERROR;
        }
    }

    // TODO: Create a fetchAll that fetches from database and caches in redis, very inefficient but useful if a struct is extremely common. It should only be used in the start of the plugin.

    public FetchResult nextInRedis() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(getClass());
            if (structInfo == null)
                throw new IllegalStateException("Struct " + getClass().getSimpleName() + " was not registered!");

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            if (this.currentFetcher == null)
                throw new IllegalArgumentException("No current fetcher was set! RedbitVolatileStruct#fetchAll(String) should do it");

            String nextKey = this.currentFetcher.next();

            if (nextKey == null) {
                this.currentFetcher.close();
                this.currentFetcher = null;

                return FetchResult.COMPLETE;
            }

            Map<String, String> valueMap = jedis.hgetAll(nextKey);

            if (valueMap.isEmpty())
                return FetchResult.NOT_FOUND;

            setFieldsValueFromRedis(structInfo, valueMap);

            return FetchResult.FOUND;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return FetchResult.ERROR;
        }
    }

    @Override
    public void close() {
        if (this.currentFetcher != null) this.currentFetcher.close();
        this.currentFetcher = null;
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

}
