package de.doridian.dependencies.redis;

import de.doridian.dependencies.config.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.*;

public class RedisManager {
    private static JedisPool jedisPool;

    private static String REDIS_PASSWORD;
    private static int REDIS_DB;

    public static void initialize(Configuration configuration) {
        REDIS_PASSWORD = configuration.getValue("redis-pw", "password");
        REDIS_DB = Integer.parseInt(configuration.getValue("redis-db", "1"));
        jedisPool = createPool(configuration.getValue("redis-host", "localhost"));
    }

    private static JedisPool createPool(String host) {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(500);
        jedisPoolConfig.setMaxIdle(100);
        jedisPoolConfig.setMaxWaitMillis(1000);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setTestWhileIdle(true);
        return new JedisPool(jedisPoolConfig, host, 6379, 1000, REDIS_PASSWORD, REDIS_DB);
    }

    public static long hlen(String key) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();

                long ret;
                if (!jedis.exists(key))
                    ret = 0L;
                else
                    ret = jedis.hlen(key);

                jedisPool.returnResource(jedis);

                return ret;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static boolean hexists(String key, String index) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();
                boolean exists = jedis.hexists(key, index);
                jedisPool.returnResource(jedis);
                return exists;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static String hget(String key, String index) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();
                String value = jedis.hget(key, index);
                jedisPool.returnResource(jedis);
                return value;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static void hset(String key, String index, String value) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();
                jedis.hset(key, index, value);
                jedisPool.returnResource(jedis);
                return;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static void hdel(String key, String index) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();
                jedis.hdel(key, index);
                jedisPool.returnResource(jedis);
                return;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static Set<String> hkeys(String key) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();
                Set<String> keys = jedis.hkeys(key);
                jedisPool.returnResource(jedis);
                return keys;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static List<String> hvals(String key) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();
                List<String> values = jedis.hvals(key);
                jedisPool.returnResource(jedis);
                return values;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static Map<String, String> hgetAll(String key) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();
                Map<String, String> values = jedis.hgetAll(key);
                jedisPool.returnResource(jedis);
                return values;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static void subscribe(String key, JedisPubSub listener) throws Exception {
        if(jedisPool == null)
            return;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.subscribe(listener, key);
            jedisPool.returnBrokenResource(jedis);
        } catch (Exception e) {
            if(jedis != null)
                jedisPool.returnBrokenResource(jedis);
            throw e;
        }
    }

    public static void publish(String key, String value) {
        Jedis jedis = null;
        while(true) {
            try {
                jedis = jedisPool.getResource();
                jedis.publish(key, value);
                jedisPool.returnResource(jedis);
                return;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
    }

    public static class RedisMap implements Map<String, String> {
        private final String name;
        public RedisMap(String name) {
            this.name = name;
        }

        @Override
        public int size() {
            return (int)hlen(name);
        }

        @Override
        public boolean isEmpty() {
            return (size() <= 0);
        }

        @Override
        public boolean containsKey(Object key) {
            return hexists(name, key.toString());
        }

        @Override
        public boolean containsValue(Object value) {
            return values().contains(value.toString());
        }

        @Override
        public String get(Object key) {
            return hget(name, key.toString());
        }

        @Override
        public Set<String> keySet() {
            Set<String> keys = hkeys(name);
            if(keys == null)
                return new HashSet<>();
            return keys;
        }

        @Override
        public Collection<String> values() {
            Collection<String> values = hvals(name);
            if(values == null)
                return Collections.emptyList();
            return values;
        }

        @Override
        public Set<Entry<String, String>> entrySet() {
            Map<String, String> entryMap = hgetAll(name);
            if(entryMap == null)
                return Collections.emptySet();
            return entryMap.entrySet();
        }

        @Override
        public String put(String key, String value) {
            String old = get(key);
            hset(name, key, value);
            return old;
        }

        @Override
        public String remove(Object key) {
            String old = get(key);
            hdel(name, key.toString());
            return old;
        }

        @Override
        public void putAll(Map<? extends String, ? extends String> m) {
            for(Entry<? extends String, ? extends String> e : m.entrySet()) {
                put(e.getKey(), e.getValue());
            }
        }

        @Override
        public void clear() {
            throw new RuntimeException();
        }
    }

    public static Map<String,String> createRedisMap(String name) {
        return new RedisMap(name);
    }
    public static Map<String,String> createCachedRedisMap(String name) {
        return createCachedRedisMap(name, 10000L);
    }
    public static Map<String,String> createCachedRedisMap(String name, long expiry) {
        return new CacheMap(expiry, name, new RedisMap(name));
    }
}