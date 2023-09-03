/*
 * redis-dependency - ${project.description}
 * Copyright © ${year} Doridian (git@doridian.net)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.doridian.foxbukkit.dependencies.redis;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractRedisHandler extends AbstractJedisPubSub {
    private final ArrayList<Thread> threads = new ArrayList<>();
    private final RedisManager redisManager;

    public enum RedisHandlerType {
        LIST, PUBSUB, BOTH
    }

    protected AbstractRedisHandler(final RedisManager redisManager, final String channelName) {
        this(redisManager, RedisHandlerType.PUBSUB, channelName);
    }

	protected AbstractRedisHandler(final RedisManager redisManager, final RedisHandlerType type, final String channelName) {
        this.redisManager = redisManager;
        if(type == RedisHandlerType.BOTH || type == RedisHandlerType.PUBSUB) {
            Thread t = redisManager.threadCreator.createThread(new Runnable() {
                public void run() {
                    while (redisManager.running) {
                        try {
                            Thread.sleep(1000);
                            redisManager.subscribe(channelName, AbstractRedisHandler.this);
                        } catch (RedisManager.PoolClosedException e) {
                            return;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            t.setName("RedisHandlerThread-subscribe-" + channelName);
            t.setDaemon(true);
            t.start();
            redisManager.addThread(t);
        }
        if(type == RedisHandlerType.BOTH || type == RedisHandlerType.LIST) {
            Thread t = redisManager.threadCreator.createThread(new Runnable() {
                public void run() {
                    while (redisManager.running) {
                        try {
                            List<String> ret = redisManager.brpop(0, channelName);
                            if(ret != null)
                                onMessage(ret.get(0), ret.get(1));
                        } catch (RedisManager.PoolClosedException e) {
                            return;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            t.setName("RedisHandlerThread-list-" + channelName);
            t.setDaemon(true);
            t.start();
            redisManager.addThread(t);
        }
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
}
