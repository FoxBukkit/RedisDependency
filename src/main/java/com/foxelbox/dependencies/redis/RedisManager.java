/**
 * This file is part of RedisDependency.
 *
 * RedisDependency is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * RedisDependency is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with RedisDependency.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.foxelbox.dependencies.redis;

import com.foxelbox.dependencies.config.Configuration;
import com.foxelbox.dependencies.threading.IThreadCreator;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.*;

public class RedisManager {
    private final ArrayList<Thread> threads = new ArrayList<>();
    private final ArrayList<Jedis> subscriptions = new ArrayList<>();
    private JedisPool jedisPool;

    private final String REDIS_PASSWORD;
    private final int REDIS_DB;

    private boolean running = true;

    public boolean isRunning() {
        return running;
    }

    public void addThread(Thread t) {
        threads.add(t);
    }

    final IThreadCreator threadCreator;

    public RedisManager(IThreadCreator _threadCreator, Configuration configuration) {
		threadCreator = _threadCreator;
        REDIS_PASSWORD = configuration.getValue("redis-pw", "password");
        REDIS_DB = Integer.parseInt(configuration.getValue("redis-db", "1"));
        createPool(configuration.getValue("redis-host", "localhost"));
    }

    public void stop() {
        running = false;
        if(jedisPool != null) {
            jedisPool.destroy();
            jedisPool = null;
        }
        for(Jedis jedis : subscriptions) {
            try {
                jedis.disconnect();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        subscriptions.clear();
        for(Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        threads.clear();
    }

    private void createPool(final String host) {
		Thread t = threadCreator.createThread(new Runnable() {
			@Override
			public void run() {
				JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
				jedisPoolConfig.setMaxTotal(500);
				jedisPoolConfig.setMaxIdle(100);
				jedisPoolConfig.setMaxWaitMillis(1000);
				jedisPoolConfig.setTestOnBorrow(true);
				jedisPoolConfig.setTestOnReturn(true);
				jedisPoolConfig.setTestWhileIdle(true);
				jedisPool = new JedisPool(jedisPoolConfig, host, 6379, 1000, REDIS_PASSWORD, REDIS_DB);
			}
		});
		t.start();
		try {
			t.join();
		} catch (InterruptedException e) { }
    }

    public long hlen(String key) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public List<String> brpop(int timeout, String... key) {
        Jedis jedis = null;
        while(running) {
            try {
                jedis = jedisPool.getResource();
                List<String> ret = jedis.brpop(timeout, key);
                jedisPool.returnResource(jedis);
                return ret;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
        throw new RuntimeException("closed");
    }

    public void del(String key) {
        Jedis jedis = null;
        while(running) {
            try {
                jedis = jedisPool.getResource();
                jedis.del(key);
                jedisPool.returnResource(jedis);
                return;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
        throw new RuntimeException("closed");
    }

    public List<String> lrange(String key, long start, long stop) {
        Jedis jedis = null;
        while(running) {
            try {
                jedis = jedisPool.getResource();
                List<String> range = jedis.lrange(key, start, stop);
                jedisPool.returnResource(jedis);
                return range;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
        throw new RuntimeException("closed");
    }

    public boolean hexists(String key, String index) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public String hget(String key, String index) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public void hset(String key, String index, String value) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public void hdel(String key, String index) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public Set<String> hkeys(String key) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public List<String> hvals(String key) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public Map<String, String> hgetAll(String key) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public void subscribe(String key, JedisPubSub listener) throws Exception {
        if(jedisPool == null)
            return;
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            subscriptions.add(jedis);
            jedis.subscribe(listener, key);
            jedisPool.returnBrokenResource(jedis);
        } catch (Exception e) {
            if(jedis != null)
                jedisPool.returnBrokenResource(jedis);
            throw e;
        } finally {
            if(jedis != null)
                subscriptions.remove(jedis);
        }
        throw new RuntimeException("closed");
    }

    public void publish(String key, String value) {
        Jedis jedis = null;
        while(running) {
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
        throw new RuntimeException("closed");
    }

    public void lpush(String key, String... strings) {
        Jedis jedis = null;
        while(running) {
            try {
                jedis = jedisPool.getResource();
                jedis.lpush(key, strings);
                jedisPool.returnResource(jedis);
                return;
            } catch (Exception e) {
                e.printStackTrace();
                if(jedis != null)
                    jedisPool.returnBrokenResource(jedis);
            }
        }
        throw new RuntimeException("closed");
    }

    public class RedisMap implements Map<String, String> {
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

    public RedisMap createRedisMap(String name) {
        return new RedisMap(name);
    }
    public CacheMap createCachedRedisMap(String name) {
        return createCachedRedisMap(name, 10000L);
    }
    public CacheMap createCachedRedisMap(String name, long expiry) {
        return new CacheMap(this, expiry, name, new RedisMap(name));
    }
}