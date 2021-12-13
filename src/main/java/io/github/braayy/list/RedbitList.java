package io.github.braayy.list;

import io.github.braayy.Redbit;
import io.github.braayy.utils.RedbitUtils;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;

public abstract class RedbitList<T> {

    private final String key;
    private final List<T> values;

    public RedbitList(String key) {
        this.key = key;
        this.values = new ArrayList<>();
    }

    public abstract T transform(String input);

    public List<T> getValues() {
        return values;
    }

    public boolean retrieve() {
        return retrieveAll(false);
    }

    public boolean retrieveAll() {
        return retrieveAll(true);
    }

    private boolean retrieveAll(boolean clearLocalList) {
        try {
            Jedis jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            List<String> list = jedis.lrange(this.key, 0, -1);

            if (clearLocalList)
                this.values.clear();

            for (String value : list) {
                T transformed = transform(value);

                this.values.add(transformed);
            }

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

    public boolean push() {
        return pushAll(false);
    }

    public boolean pushAll() {
        return pushAll(true);
    }

    private boolean pushAll(boolean clearRedisList) {
        try {
            Jedis jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            if (clearRedisList)
                jedis.del(this.key);

            Object[] snapshot = this.values.toArray();
            jedis.lpush(this.key, RedbitUtils.toStringArray(snapshot));

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, exception.getMessage(), exception);

            return false;
        }
    }

}
