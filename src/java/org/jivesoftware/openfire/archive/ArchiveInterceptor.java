package org.jivesoftware.openfire.archive;

import org.jivesoftware.openfire.cluster.ClusterManager;
import org.jivesoftware.openfire.interceptor.InterceptorManager;
import org.jivesoftware.openfire.interceptor.PacketInterceptor;
import org.jivesoftware.openfire.interceptor.PacketRejectedException;
import org.jivesoftware.openfire.privacy.PrivacyList;
import org.jivesoftware.openfire.privacy.PrivacyListManager;
import org.jivesoftware.openfire.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.JID;
import org.xmpp.packet.Message;
import org.xmpp.packet.Packet;

import java.util.Date;

public class ArchiveInterceptor implements PacketInterceptor {

    private ConversationManager conversationManager;
    private FileArchivedMessageConsumer fileConsumer;
    private static final Logger Log = LoggerFactory.getLogger(ArchiveInterceptor.class);

    public ArchiveInterceptor(ConversationManager conversationManager) {
        this.conversationManager = conversationManager;
        this.fileConsumer = new FileArchivedMessageConsumer();  // instantiate our file consumer
    }

    @Override
    public void interceptPacket(Packet packet, Session session, boolean incoming, boolean processed)
        throws PacketRejectedException {
        // Ignore any packets that haven't already been processed by interceptors.
        if (!processed) {
            return;
        }
        if (packet instanceof Message) {
            // Ignore any outgoing messages (we'll catch them when they're incoming).
            if (!incoming) {
                return;
            }
            Message message = (Message) packet;
            // Ignore any messages that don't have a body so that we skip events.
            if (message.getBody() != null) {
                // Only process messages that are between two users, group chat rooms, or gateways.
                if (conversationManager.isConversation(message)) {
                    // Take care on blocklist
                    JID to = message.getTo();
                    if (to != null) {
                        final PrivacyList defaultPrivacyList = PrivacyListManager.getInstance().getDefaultPrivacyList(to.getNode());
                        if (defaultPrivacyList != null && defaultPrivacyList.shouldBlockPacket(message)) {
                            Log.debug("Not storing message, as it is rejected by the default privacy list of the recipient ({}).", to.getNode());
                            return;
                        }
                    }
                    // Process this event in the senior cluster member or local JVM when not in a cluster
                    if (ClusterManager.isSeniorClusterMember()) {
                        Date currentDate = new Date();
                        Conversation conversation = conversationManager.getConversation(message.getFrom(), message.getTo(), currentDate);
                        if (conversation != null) {
                            long conversationId = conversation.getConversationID();
                            boolean isRead = true;
                            JID serverJID = null;

                            ArchivedMessage archivedMessage = new ArchivedMessage(conversationId, message.getFrom(), message.getTo(), currentDate, message.getBody(), isRead, serverJID);
                            fileConsumer.consumeMessage(archivedMessage);
                        } else {
                            Log.warn("Unable to find or create a conversation for the given message.");
                        }
                    } else {
                        JID sender = message.getFrom();
                        JID receiver = message.getTo();
                        ConversationEventsQueue eventsQueue = conversationManager.getConversationEventsQueue();
                        eventsQueue.addChatEvent(conversationManager.getConversationKey(sender, receiver),
                            ConversationEvent.chatMessageReceived(sender, receiver,
                                conversationManager.isMessageArchivingEnabled() ? message.getBody() : null,
                                conversationManager.isMessageArchivingEnabled() ? message.toXML() : null,
                                new Date()));
                    }
                }
            }
        }
    }

    public void start() {
        InterceptorManager.getInstance().addInterceptor(this);
    }

    public void stop() {
        InterceptorManager.getInstance().removeInterceptor(this);
        conversationManager = null;
    }
}
