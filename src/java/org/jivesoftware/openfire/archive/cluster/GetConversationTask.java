/*
 * Copyright (C) 2008 Jive Software. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jivesoftware.openfire.archive.cluster;

import org.jivesoftware.openfire.XMPPServer;
import org.jivesoftware.openfire.archive.ConversationManager;
import org.jivesoftware.openfire.archive.MonitoringConstants;
import org.jivesoftware.openfire.container.Plugin;
import org.jivesoftware.openfire.plugin.MonitoringPlugin;
import org.jivesoftware.util.NotFoundException;
import org.jivesoftware.util.cache.ClusterTask;
import org.jivesoftware.util.cache.ExternalizableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Optional;

/**
 * Task that returns the specified conversation or <tt>null</tt> if not found.
 *
 * This task intentionally works String (XML) representations instead of the Conversation objects themselves. This
 * prevents classloader issues that may otherwise occur when the plugin is reloaded.

 * @author Gaston Dombiak
 */
public class GetConversationTask implements ClusterTask<String>
{
    private static final Logger Log = LoggerFactory.getLogger(GetConversationTask.class);

    private long conversationID;
    private String conversationXml;

    public GetConversationTask() {
    }

    public GetConversationTask(final long conversationID) {
        this.conversationID = conversationID;
    }

    public String getResult() {
        return conversationXml;
    }

    public void run() {
        final Optional<Plugin> plugin = XMPPServer.getInstance().getPluginManager().getPluginByName(MonitoringConstants.PLUGIN_NAME);
        if (!plugin.isPresent()) {
            Log.error("Unable to execute cluster task! The Monitoring plugin does not appear to be loaded on this machine.");
            return;
        }
        final ConversationManager conversationManager = ((MonitoringPlugin)plugin.get()).getConversationManager();
        try {
            // ClassCastExceptions occur when using classes provided by a plugin during serialization (sometimes only after
            // reloading the plugin without restarting Openfire. This is why this implementation marshalls data as XML when
            // serializing. See https://github.com/igniterealtime/openfire-monitoring-plugin/issues/120
            // and https://github.com/igniterealtime/openfire-monitoring-plugin/issues/156
            conversationXml = conversationManager.getConversation(conversationID).toXml();
        } catch (NotFoundException | IOException e) {
            Log.debug("Exception occurred while running GetConversationTask.", e);
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        ExternalizableUtil.getInstance().writeLong(out, conversationID);
    }

    public void readExternal(ObjectInput in) throws IOException {
        conversationID = ExternalizableUtil.getInstance().readLong(in);
    }
}
