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
