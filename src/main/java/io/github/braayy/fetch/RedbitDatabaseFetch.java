package io.github.braayy.fetch;

import io.github.braayy.Redbit;
import io.github.braayy.RedbitQuery;
import io.github.braayy.struct.RedbitStruct;
import io.github.braayy.struct.RedbitStructInfo;
import io.github.braayy.utils.RedbitUtils;
import redis.clients.jedis.JedisPooled;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;

public class RedbitDatabaseFetch extends RedbitFetch {

    private final RedbitQuery query;

    public RedbitDatabaseFetch(RedbitStruct struct, RedbitQuery query) {
        super(struct);
        this.query = query;
    }

    @Override
    public Result next() {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(struct.getClass());
            Objects.requireNonNull(structInfo, "Struct " + struct.getClass().getSimpleName() + " was not registered!");

            if (this.query == null)
                throw new IllegalArgumentException("No current query was set! RedbitStruct#customFetch(String) should do it");

            if (this.query.getStatement().isClosed())
                throw new IllegalArgumentException("Current Query's Statement is closed!");

            ResultSet resultSet = this.query.getResultSet();

            if (resultSet == null) {
                this.query.close();
                throw new IllegalArgumentException("Current Query has no Result Set");
            }

            if (!resultSet.next()) {
                this.query.close();
                return Result.COMPLETE;
            }

            RedbitUtils.setFieldsValueFromResultSet(structInfo, struct, resultSet, true);

            return Result.FOUND;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return Result.ERROR;
        }
    }

    public boolean cache(boolean overwrite) {
        try {
            RedbitStructInfo structInfo = Redbit.getStructRegistry().getStructInfo(struct.getClass());
            Objects.requireNonNull(structInfo, "Struct " + struct.getClass().getSimpleName() + " was not registered!");

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            Map<String, String> valueMap = RedbitUtils.getStructValues(structInfo, struct, false);

            String idValue = valueMap.remove(structInfo.getIdColumn().getName());
            if (RedbitUtils.isNullString(idValue))
                throw new IllegalArgumentException("Invalid id value for struct " + structInfo.getName());

            String key = String.format(Redbit.KEY_FORMAT, structInfo.getName(), idValue);

            if (jedis.exists(key) && !overwrite) return true;

            jedis.hset(key, valueMap);
            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        if (this.query != null) this.query.close();
    }

}
