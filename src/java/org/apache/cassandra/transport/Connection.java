/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport;

import org.jboss.netty.channel.Channel;

public class Connection
{
    private volatile FrameCompressor frameCompressor;
    protected volatile Channel channel;
    private final Tracker tracker;

    public Connection(Tracker tracker)
    {
        this.tracker = tracker;
    }

    public void setCompressor(FrameCompressor compressor)
    {
        this.frameCompressor = compressor;
    }

    public FrameCompressor getCompressor()
    {
        return frameCompressor;
    }

    public Tracker getTracker()
    {
        return tracker;
    }

    public void registerChannel(Channel ch)
    {
        channel = ch;
        tracker.addConnection(ch, this);
    }

    public Channel channel()
    {
        return channel;
    }

    public interface Factory
    {
        public Connection newConnection(Tracker tracker);
    }

    public interface Tracker
    {
        public void addConnection(Channel ch, Connection connection);
        public void closeAll();
    }
}
