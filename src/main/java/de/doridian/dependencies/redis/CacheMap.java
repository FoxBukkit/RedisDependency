package de.doridian.dependencies.redis;

import redis.clients.jedis.JedisPubSub;

import java.util.*;

public class CacheMap implements Map<String, String> {
    private final long expiryTime;
    private final String name;

    private final JedisPubSubListener jedisPubSubListener;

    private class JedisPubSubListener extends AbstractRedisHandler {
        private JedisPubSubListener(String channelName) {
            super(channelName);
        }

        @Override
        public void onMessage(final String c_message) {
            final String[] msgSplit = c_message.split("\0");
            synchronized (internalMap) {
                switch (msgSplit.length) {
                    case 1:
                        if(msgSplit[0].charAt(0) == '\1')
                            internalMap.clear();
                        else
                            internalMap.remove(msgSplit[0]);
                        break;
                    case 2:
                        internalMap.put(msgSplit[0], new CacheEntry(msgSplit[1]));
                        break;
                }
            }
        }
    }

    public CacheMap(final long expiryTime, final String _name, final Map<String, String> parentMap) {
        this.expiryTime = expiryTime;
        this.parentMap = parentMap;
        this.name = "cachemap_changes:" + _name;

        Thread cleanupThread = new Thread() {
            @Override
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(expiryTime / 2L);
                        final long currentTime = System.currentTimeMillis();
                        synchronized (internalMap) {
                            final Set<String> keysToRemove = new HashSet<>();
                            for(Entry<String, CacheEntry> entry : internalMap.entrySet())
                                if(entry.getValue().expiry < currentTime)
                                    keysToRemove.add(entry.getKey());
                            for(String key : keysToRemove)
                                internalMap.remove(key);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        cleanupThread.setName("RedisCacheMapCleanupThread-" + this.name);
        cleanupThread.setDaemon(true);
        cleanupThread.start();

        this.jedisPubSubListener = new JedisPubSubListener(this.name);
    }

    private class CacheEntry {
        private final String data;
        private final long expiry;
        private CacheEntry(String data) {
            this.data = data;
            this.expiry = System.currentTimeMillis() + expiryTime;
        }

        private boolean isExpired() {
            return expiry < System.currentTimeMillis();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof CacheEntry)) return false;

            CacheEntry that = (CacheEntry) o;

            if (data != null ? !data.equals(that.data) : that.data != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return data != null ? data.hashCode() : 0;
        }
    }
    private final HashMap<String, CacheEntry> internalMap =  new HashMap<>();
    private final Map<String, String> parentMap;

    @Override
    public int size() {
        synchronized (parentMap) {
            return parentMap.size();
        }
    }

    @Override
    public boolean isEmpty() {
        synchronized (parentMap) {
            return parentMap.isEmpty();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        synchronized (parentMap) {
            return parentMap.containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        synchronized (parentMap) {
            return parentMap.containsValue(value);
        }
    }

    @Override
    public String get(Object key) {
        CacheEntry cacheEntry;
        synchronized (internalMap) {
            cacheEntry = internalMap.get(key);
        }
        if(cacheEntry != null && !cacheEntry.isExpired())
            return cacheEntry.data;
        final String value;
        synchronized (parentMap) {
            value = parentMap.get(key);
        }
        cacheEntry = new CacheEntry(value);
        synchronized (internalMap) {
            internalMap.put(key.toString(), cacheEntry);
        }
        return value;
    }

    @Override
    public String put(String key, String value) {
        synchronized (internalMap) {
            internalMap.put(key, new CacheEntry(value));
        }
        RedisManager.publish(name, key + '\0' + value);
        synchronized (parentMap) {
            return parentMap.put(key, value);
        }
    }

    @Override
    public String remove(Object key) {
        synchronized (internalMap) {
            internalMap.remove(key);
        }
        RedisManager.publish(name, key.toString());
        synchronized (parentMap) {
            return parentMap.remove(key);
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        for(Entry<? extends String,? extends String> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        synchronized (internalMap) {
            internalMap.clear();
        }
        RedisManager.publish(name, "\1");
        synchronized (parentMap) {
            parentMap.clear();
        }
    }

    @Override
    public Set<String> keySet() {
        synchronized (parentMap) {
            return parentMap.keySet();
        }
    }

    @Override
    public Collection<String> values() {
        synchronized (parentMap) {
            return parentMap.values();
        }
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        synchronized (parentMap) {
            return parentMap.entrySet();
        }
    }
}
