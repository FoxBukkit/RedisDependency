package de.doridian.dependencies.redis;

import redis.clients.jedis.JedisPubSub;

public abstract class AbstractRedisHandler extends JedisPubSub {
	protected AbstractRedisHandler(final RedisManager redisManager, final String channelName) {
        final JedisPubSub _this = this;
        Thread t = new Thread() {
            public void run() {
                while(true) {
                    try {
                        Thread.sleep(1000);
                        redisManager.subscribe(channelName, _this);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        t.setName("RedisHandlerThread-" + channelName);
        t.setDaemon(true);
        t.start();
	}

    protected abstract void onMessage(final String message) throws Exception;

	@Override
	public final void onMessage(final String channel, final String c_message) {
		try {
            onMessage(c_message);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    @Override public final void onPMessage(String pattern, String channel, String message) { }
    @Override public final void onSubscribe(String channel, int subscribedChannels) { }
    @Override public final void onUnsubscribe(String channel, int subscribedChannels) { }
    @Override public final void onPUnsubscribe(String pattern, int subscribedChannels) { }
    @Override public final void onPSubscribe(String pattern, int subscribedChannels) { }
}
