package io.github.braayy.set;

import com.google.common.collect.Sets;
import io.github.braayy.Redbit;
import redis.clients.jedis.JedisPooled;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.logging.Level;
import java.util.stream.Collectors;

public abstract class RedbitSet<T> {

    private final String key;

    public RedbitSet(String name, String id) {
        this.key = String.format(Redbit.KEY_FORMAT, name, id);
    }

    public abstract T fromString(String input);

    public abstract String toString(T input);

    @SafeVarargs
    public final boolean add(@Nonnull T... values) {
        return add(Sets.newHashSet(values));
    }

    public boolean add(Set<T> elements) {
        try {
            if (elements.size() == 0) return true;

            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            String[] strings = elements.stream().map(this::toString).toArray(String[]::new);

            jedis.sadd(this.key, strings);

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while adding item to redbit set", exception);

            return false;
        }
    }

    public boolean remove(@Nonnull T value) {
        try {
            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            jedis.srem(this.key, toString(value));

            return true;
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while removing item from redbit set", exception);

            return false;
        }
    }

    public Set<T> fetch() {
        try {
            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            Set<String> range = jedis.smembers(this.key);

            return range.stream().map(this::fromString).collect(Collectors.toSet());
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while ranging items from redbit set", exception);

            return Collections.emptySet();
        }
    }

    public long size() {
        try {
            JedisPooled jedis = Redbit.getJedis();
            Objects.requireNonNull(jedis, "Jedis was not initialized yet! Redbit#init(RedbitConfig) should do it");

            return jedis.scard(this.key);
        } catch (Exception exception) {
            Redbit.getLogger().log(Level.SEVERE, "Something went wrong while ranging items from redbit set", exception);

            return -1L;
        }
    }

}
