package io.github.braayy.list;

import io.github.braayy.Redbit;
import redis.clients.jedis.JedisPooled;

import org.jetbrains.annotations.NotNull;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

public abstract class RedbitList<T> {

    private final String key;

    public RedbitList(String name, String id) {
        this.key = String.format(Redbit.KEY_FORMAT, name, id);
    }

    public abstract T fromString(String input);

    public abstract String toString(T input);

    @SafeVarargs
    public final boolean add(@NotNull T... values) {
        return add(Arrays.asList(values));
    }

    public boolean add(@NotNull List<T> elements) {
        try {
            if (elements.size() == 0) return true;

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String[] strings = elements.stream().map(this::toString).toArray(String[]::new);

            jedis.lpush(this.key, strings);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while adding item to redbit list", exception);

            return false;
        }
    }

    public boolean remove(@NotNull T value) {
        try {
            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            jedis.lrem(this.key, 1, toString(value));

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while removing item from redbit list", exception);

            return false;
        }
    }

    @NotNull
    public List<T> range(int start, int end) {
        try {
            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            List<String> range = jedis.lrange(this.key, start, end);

            return range.stream().map(this::fromString).collect(Collectors.toList());
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while ranging items from redbit list", exception);

            return Collections.emptyList();
        }
    }

    public long size() {
        try {
            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            return jedis.llen(this.key);
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while ranging items from redbit list", exception);

            return -1L;
        }
    }

}
