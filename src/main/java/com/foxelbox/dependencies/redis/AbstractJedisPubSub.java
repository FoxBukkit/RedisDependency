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

import redis.clients.jedis.JedisPubSub;

public class AbstractJedisPubSub extends JedisPubSub {
	@Override public void onMessage(String channel, String message) { }
	@Override public void onPMessage(String pattern, String channel, String message) { }
	@Override public void onSubscribe(String channel, int subscribedChannels) { }
	@Override public void onUnsubscribe(String channel, int subscribedChannels) { }
	@Override public void onPUnsubscribe(String pattern, int subscribedChannels) { }
	@Override public void onPSubscribe(String pattern, int subscribedChannels) { }
}
