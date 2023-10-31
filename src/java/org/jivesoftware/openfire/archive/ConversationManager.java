

package org.jivesoftware.openfire.archive;

import com.reucon.openfire.plugin.archive.util.StanzaIDUtil;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.jivesoftware.database.DbConnectionManager;
import org.jivesoftware.openfire.XMPPServer;
import org.jivesoftware.openfire.XMPPServerInfo;
import org.jivesoftware.openfire.archive.cluster.GetConversationCountTask;
import org.jivesoftware.openfire.archive.cluster.GetConversationTask;
import org.jivesoftware.openfire.archive.cluster.GetConversationsTask;
import org.jivesoftware.openfire.archive.cluster.GetConversationsWriteETATask;
import org.jivesoftware.openfire.cluster.ClusterManager;
import org.jivesoftware.openfire.component.ComponentEventListener;
import org.jivesoftware.openfire.component.InternalComponentManager;
import org.jivesoftware.openfire.muc.MultiUserChatService;
import org.jivesoftware.openfire.plugin.MonitoringPlugin;
import org.jivesoftware.openfire.reporting.util.TaskEngine;
import org.jivesoftware.openfire.stats.Statistic;
import org.jivesoftware.openfire.stats.StatisticsManager;
import org.jivesoftware.util.*;
import org.jivesoftware.util.cache.CacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.IQ;
import org.xmpp.packet.JID;
import org.xmpp.packet.Message;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;


public class ConversationManager implements ComponentEventListener{

    private static final Logger Log = LoggerFactory.getLogger(ConversationManager.class);

    private static final String UPDATE_CONVERSATION = "UPDATE ofConversation SET lastActivity=?, messageCount=? WHERE conversationID=?";
    private static final String UPDATE_PARTICIPANT = "UPDATE ofConParticipant SET leftDate=? WHERE conversationID=? AND bareJID=? AND jidResource=? AND joinedDate=?";
    private static final String INSERT_MESSAGE = "INSERT INTO ofMessageArchive(messageID, conversationID, fromJID, fromJIDResource, toJID, toJIDResource, sentDate, body, stanza, isPMforJID) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?)";
    private static final String CONVERSATION_COUNT = "SELECT COUNT(*) FROM ofConversation";
    private static final String MESSAGE_COUNT = "SELECT COUNT(*) FROM ofMessageArchive";
    private static final String DELETE_CONVERSATION_1 = "DELETE FROM ofMessageArchive WHERE conversationID=?";
    private static final String DELETE_CONVERSATION_2 = "DELETE FROM ofConParticipant WHERE conversationID=?";
    private static final String DELETE_CONVERSATION_3 = "DELETE FROM ofConversation WHERE conversationID=?";

    private static final Duration DEFAULT_IDLE_TIME = Duration.ofMinutes(10);
    private static final Duration DEFAULT_MAX_TIME = Duration.ofMinutes(60);
    public static final Duration DEFAULT_MAX_TIME_DEBUG = Duration.ofMinutes(30);

    public static final Duration DEFAULT_MAX_RETRIEVABLE = Duration.ofDays(0);
    private static final Duration DEFAULT_MAX_AGE = Duration.ofDays(0);

    public static final String CONVERSATIONS_KEY = "conversations";

    private ConversationEventsQueue conversationEventsQueue;
    private TaskEngine taskEngine;

    private Map<String, Conversation> conversations = new ConcurrentHashMap<>();
    private boolean metadataArchivingEnabled;
    /**
     * Flag that indicates if messages of one-to-one chats should be archived.
     */
    private boolean messageArchivingEnabled;
    /**
     * Flag that indicates if messages of group chats (in MUC rooms) should be archived.
     */
    private boolean roomArchivingEnabled;
    private boolean roomArchivingStanzasEnabled;
    /**
     * List of room names to archive. When list is empty then all rooms are archived (if roomArchivingEnabled is enabled).
     */
    private Collection<String> roomsArchived;
    private Duration idleTime;
    private Duration maxTime;
    private Duration maxAge;
    private Duration maxRetrievable;
    private PropertyEventListener propertyListener;

    private TimerTask cleanupTask;
    private TimerTask maxAgeTask;

    private Collection<ConversationListener> conversationListeners;

    /**
     * Keeps the address of those components that provide the gateway service.
     */
    private List<String> gateways;
    private XMPPServerInfo serverInfo;

    private Archiver<Conversation> conversationArchiver;
    private Archiver<ArchivedMessage> messageArchiver;
    private Archiver<RoomParticipant> participantArchiver;

    public static SystemProperty<Boolean> METADATA_ARCHIVING_ENABLED = SystemProperty.Builder.ofType(Boolean.class)
        .setKey("conversation.metadataArchiving")
        .setDefaultValue(true)
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();

    public static SystemProperty<Boolean> MESSAGE_ARCHIVING_ENABLED = SystemProperty.Builder.ofType(Boolean.class)
        .setKey("conversation.messageArchiving")
        .setDefaultValue(false)
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();

    public static SystemProperty<Boolean> ROOM_ARCHIVING_ENABLED = SystemProperty.Builder.ofType(Boolean.class)
        .setKey("conversation.roomArchiving")
        .setDefaultValue(false)
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();

    public static SystemProperty<Boolean> ROOM_STANZA_ARCHIVING_ENABLED = SystemProperty.Builder.ofType(Boolean.class)
        .setKey("conversation.roomArchivingStanzas")
        .setDefaultValue(false)
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();

    public static SystemProperty<String> ROOMS_ARCHIVED = SystemProperty.Builder.ofType(String.class)
        .setKey("conversation.roomsArchived")
        .setDefaultValue("")
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();

    public static SystemProperty<Duration> IDLE_TIME = SystemProperty.Builder.ofType(Duration.class)
        .setKey("conversation.idleTime")
        .setDefaultValue(DEFAULT_IDLE_TIME)
        .setChronoUnit(ChronoUnit.MINUTES)
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();

    public static SystemProperty<Duration> MAX_TIME = SystemProperty.Builder.ofType(Duration.class)
        .setKey("conversation.maxTime")
        .setDefaultValue(DEFAULT_MAX_TIME)
        .setChronoUnit(ChronoUnit.MINUTES)
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();
    
    public static SystemProperty<Duration> MAX_AGE = SystemProperty.Builder.ofType(Duration.class)
        .setKey("conversation.maxAge")
        .setDefaultValue(DEFAULT_MAX_AGE)
        .setChronoUnit(ChronoUnit.DAYS)
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();

    public static SystemProperty<Duration> MAX_RETRIEVABLE = SystemProperty.Builder.ofType(Duration.class)
        .setKey("conversation.maxRetrievable")
        .setDefaultValue(DEFAULT_MAX_RETRIEVABLE)
        .setChronoUnit(ChronoUnit.DAYS)
        .setDynamic(true)
        .setPlugin(MonitoringConstants.PLUGIN_NAME)
        .build();

    public ConversationManager(TaskEngine taskEngine) {
        this.taskEngine = taskEngine;
        this.gateways = new CopyOnWriteArrayList<>();
        this.serverInfo = XMPPServer.getInstance().getServerInfo();
        this.conversationEventsQueue = new ConversationEventsQueue(this, taskEngine);
    }

    public void start() {
        metadataArchivingEnabled = METADATA_ARCHIVING_ENABLED.getValue();
        messageArchivingEnabled = MESSAGE_ARCHIVING_ENABLED.getValue();
        if (messageArchivingEnabled && !metadataArchivingEnabled) {
            Log.warn("Metadata archiving must be enabled when message archiving is enabled. Overriding setting.");
            metadataArchivingEnabled = true;
        }
        roomArchivingEnabled = ROOM_ARCHIVING_ENABLED.getValue();
        roomArchivingStanzasEnabled = ROOM_STANZA_ARCHIVING_ENABLED.getValue();
        roomsArchived = StringUtils.stringToCollection(ROOMS_ARCHIVED.getValue());
        if (roomArchivingEnabled && !metadataArchivingEnabled) {
            Log.warn("Metadata archiving must be enabled when room archiving is enabled. Overriding setting.");
            metadataArchivingEnabled = true;
        }
        idleTime = IDLE_TIME.getValue();
        maxTime = MAX_TIME.getValue();

        maxAge = MAX_AGE.getValue();
        maxRetrievable = MAX_RETRIEVABLE.getValue();

        // Listen for any changes to the conversation properties.
        propertyListener = new ConversationPropertyListener();
        PropertyEventDispatcher.addListener(propertyListener);

        conversationListeners = new CopyOnWriteArraySet<>();

        conversationArchiver = new ConversationArchivingRunnable( "MonitoringPlugin Conversations" );
        messageArchiver = new MessageArchivingRunnable( "MonitoringPlugin Messages" );
        participantArchiver = new ParticipantArchivingRunnable( "MonitoringPlugin Participants" );
        XMPPServer.getInstance().getArchiveManager().add( conversationArchiver );
        XMPPServer.getInstance().getArchiveManager().add( messageArchiver );
        XMPPServer.getInstance().getArchiveManager().add( participantArchiver );

        if (JiveGlobals.getProperty("conversation.maxTimeDebug") != null) {
            Log.info("Monitoring plugin max time value deleted. Must be left over from stalled userCreation plugin run.");
            JiveGlobals.deleteProperty("conversation.maxTimeDebug");
        }
        
        // Schedule a task to do conversation cleanup.
        cleanupTask = new TimerTask() {
            @Override
            public void run() {
                for (String key : conversations.keySet()) {
                    Conversation conversation = conversations.get(key);
                    long now = System.currentTimeMillis();
                    if ((now - conversation.getLastActivity().getTime() > idleTime.toMillis()) || (now - conversation.getStartDate().getTime() > maxTime.toMillis())) {
                        removeConversation(key, conversation, new Date(now));
                    }
                }
            }
        };
        taskEngine.scheduleAtFixedRate(cleanupTask, JiveConstants.MINUTE * 5, JiveConstants.MINUTE * 5);

        // Schedule a task to do conversation purging.
        maxAgeTask = new TimerTask() {
            @Override
            public void run() {
                if (maxAge.toDays() > 0) {
                    // Delete conversations older than maxAge days
                    Connection con = null;
                    PreparedStatement pstmt1 = null;
                    PreparedStatement pstmt2 = null;
                    PreparedStatement pstmt3 = null;
                    try {
                        con = DbConnectionManager.getConnection();
                        pstmt1 = con.prepareStatement(DELETE_CONVERSATION_1);
                        pstmt2 = con.prepareStatement(DELETE_CONVERSATION_2);
                        pstmt3 = con.prepareStatement(DELETE_CONVERSATION_3);
                        Date now = new Date();
                        Date maxAgeDate = new Date(now.getTime() - maxAge.toMillis());
                        ArchiveSearch search = new ArchiveSearch();
                        search.setDateRangeMax(maxAgeDate);
                        MonitoringPlugin plugin = (MonitoringPlugin) XMPPServer.getInstance().getPluginManager().getPluginByName(MonitoringConstants.PLUGIN_NAME).get();
                        ArchiveSearcher archiveSearcher = plugin.getArchiveSearcher();
                        Collection<Conversation> conversations = archiveSearcher.search(search);
                        int conversationDeleted = 0;
                        for (Conversation conversation : conversations) {
                            Log.debug("Deleting: " + conversation.getConversationID() + " with date: " + conversation.getStartDate()
                                    + " older than: " + maxAgeDate);
                            pstmt1.setLong(1, conversation.getConversationID());
                            pstmt1.execute();
                            pstmt2.setLong(1, conversation.getConversationID());
                            pstmt2.execute();
                            pstmt3.setLong(1, conversation.getConversationID());
                            pstmt3.execute();
                            conversationDeleted++;
                        }
                        if (conversationDeleted > 0) {
                            Log.info("Deleted " + conversationDeleted + " conversations with date older than: " + maxAgeDate);
                        }
                    } catch (Exception e) {
                        Log.error(e.getMessage(), e);
                    } finally {
                        DbConnectionManager.closeConnection(pstmt1, con);
                        DbConnectionManager.closeConnection(pstmt2, con);
                        DbConnectionManager.closeConnection(pstmt3, con);
                    }
                }
            }
        };
        taskEngine.scheduleAtFixedRate(maxAgeTask, JiveConstants.MINUTE, JiveConstants.MINUTE);

        // Register a statistic.
        Statistic conversationStat = new Statistic() {

            public String getName() {
                return LocaleUtils.getLocalizedString("stat.conversation.name", MonitoringConstants.NAME);
            }

            public Type getStatType() {
                return Type.count;
            }

            public String getDescription() {
                return LocaleUtils.getLocalizedString("stat.conversation.desc", MonitoringConstants.NAME);
            }

            public String getUnits() {
                return LocaleUtils.getLocalizedString("stat.conversation.units", MonitoringConstants.NAME);
            }

            public double sample() {
                return getConversationCount();
            }

            public boolean isPartialSample() {
                return false;
            }
        };
        StatisticsManager.getInstance().addStatistic(CONVERSATIONS_KEY, conversationStat);
        InternalComponentManager.getInstance().addListener(this);
    }

    public void stop() {
        cleanupTask.cancel();
        cleanupTask = null;

        maxAgeTask.cancel();
        maxAgeTask = null;
        // Remove the statistics.
        StatisticsManager.getInstance().removeStatistic(CONVERSATIONS_KEY);

        PropertyEventDispatcher.removeListener(propertyListener);
        propertyListener = null;

        conversationListeners.clear();
        conversationListeners = null;

        serverInfo = null;
        InternalComponentManager.getInstance().removeListener(this);

        XMPPServer.getInstance().getArchiveManager().remove( conversationArchiver );
        XMPPServer.getInstance().getArchiveManager().remove( messageArchiver );
        XMPPServer.getInstance().getArchiveManager().remove( participantArchiver );
    }


    public boolean isMetadataArchivingEnabled() {
        return metadataArchivingEnabled;
    }

    /**
     * @param enabled
     */
    public void setMetadataArchivingEnabled(boolean enabled) {
        this.metadataArchivingEnabled = enabled;
        METADATA_ARCHIVING_ENABLED.setValue(enabled);
    }


    public boolean isArchivingEnabled() {
        return isMessageArchivingEnabled() || isRoomArchivingEnabled();
    }


    public boolean isMessageArchivingEnabled() {
        return messageArchivingEnabled;
    }


    public void setMessageArchivingEnabled(boolean enabled) {
        this.messageArchivingEnabled = enabled;
        MESSAGE_ARCHIVING_ENABLED.setValue(enabled);
        // Force metadata archiving enabled.
        if (enabled) {
            this.metadataArchivingEnabled = true;
        }
    }

    public boolean isRoomArchivingEnabled() {
        return roomArchivingEnabled;
    }


    public boolean isRoomArchivingStanzasEnabled() {
        return roomArchivingStanzasEnabled;
    }


    public void setRoomArchivingEnabled(boolean enabled) {
        this.roomArchivingEnabled = enabled;
        ROOM_ARCHIVING_ENABLED.setValue(enabled);
        // Force metadata archiving enabled.
        if (enabled) {
            this.metadataArchivingEnabled = true;
        }
    }
    
    public void setRoomArchivingStanzasEnabled(boolean enabled) {
        this.roomArchivingStanzasEnabled = enabled;
        ROOM_STANZA_ARCHIVING_ENABLED.setValue(enabled);
        // Force metadata archiving enabled.
    }

    public Collection<String> getRoomsArchived() {
        return roomsArchived;
    }


    public void setRoomsArchived(Collection<String> roomsArchived) {
        this.roomsArchived = roomsArchived;
        ROOMS_ARCHIVED.setValue(StringUtils.collectionToString(roomsArchived));
    }


    public Duration getIdleTime() {
        return idleTime;
    }


    public void setIdleTime(Duration idleTime) {
        if (idleTime.toMinutes() < 1) {
            throw new IllegalArgumentException("Idle time less than 1 is not valid: " + idleTime);
        }
        IDLE_TIME.setValue(idleTime);
        this.idleTime = idleTime;
    }


    public Duration getMaxTime() {
        return maxTime;
    }


    public void setMaxTime(Duration maxTime) {
        if (maxTime.toMinutes() < 1) {
            throw new IllegalArgumentException("Max time less than 1 is not valid: " + maxTime);
        }
        MAX_TIME.setValue(maxTime);
        this.maxTime = maxTime;
    }

    public Duration getMaxAge() {
        return maxAge;
    }

    public void setMaxAge(Duration maxAge) {
        if (maxAge.toDays() < 0) {
            throw new IllegalArgumentException("Max age less than 0 is not valid: " + maxAge);
        }
        MAX_AGE.setValue(maxAge);
        this.maxAge = maxAge;
    }

    public Duration getMaxRetrievable() {
        return maxRetrievable;
    }

    public void setMaxRetrievable(Duration maxRetrievable) {
        if (maxRetrievable.toDays() < 0) {
            throw new IllegalArgumentException("Max retrievable less than 0 is not valid: " + maxRetrievable);
        }
        MAX_RETRIEVABLE.setValue(maxRetrievable);
        this.maxRetrievable = maxRetrievable;
    }

    public ConversationEventsQueue getConversationEventsQueue() {
        return conversationEventsQueue;
    }


    public int getConversationCount() {
        if (ClusterManager.isSeniorClusterMember()) {
            return conversations.size();
        }
        final Integer count = CacheFactory.doSynchronousClusterTask(new GetConversationCountTask(), ClusterManager.getSeniorClusterMember().toByteArray());
        if (count == null) {
            Log.warn("Unable to obtain conversation count from senior cluster member. Is (the same version of) the Monitoring Plugin running there?");
            return -1;
        } else {
            return count;
        }
    }


    public Conversation getConversation(long conversationID) throws NotFoundException {
        if (ClusterManager.isSeniorClusterMember()) {
            // Search through the currently active conversations.
            for (Conversation conversation : conversations.values()) {
                if (conversation.getConversationID() == conversationID) {
                    return conversation;
                }
            }
            // Otherwise, it might be an archived conversation, so attempt to load it.
            return ConversationDAO.loadConversation(conversationID);
        } else {
            // Get this info from the senior cluster member when running in a cluster
            String conversationXml = CacheFactory.doSynchronousClusterTask(new GetConversationTask(conversationID), ClusterManager
                    .getSeniorClusterMember().toByteArray());
            if (conversationXml == null) {
                throw new NotFoundException("Conversation not found: " + conversationID);
            }
            try {
                return Conversation.fromXml(conversationXml);
            } catch (IOException e) {
                Log.warn("Conversation {} could not be reconstructed from '{}' because of '{}'. Handling this as if the conversation was not found.", conversationID, conversationXml, e.getMessage());
                throw new NotFoundException("Conversation not found: " + conversationID);
            }
        }
    }



    public Collection<Conversation> getConversations() {
        if (ClusterManager.isSeniorClusterMember()) {
            List<Conversation> conversationList = new ArrayList<>(conversations.values());
            // Sort the conversations by creation date.
            conversationList.sort(Comparator.comparing(Conversation::getStartDate));
            return conversationList;
        } else {
            // Get this info from the senior cluster member when running in a cluster
            Collection<String> conversationXmls = CacheFactory.doSynchronousClusterTask(new GetConversationsTask(), ClusterManager
                    .getSeniorClusterMember().toByteArray());
            Collection<Conversation> result = new ArrayList<>();
            if (conversationXmls == null) {
                Log.warn("Unable to obtain conversations from senior cluster member. Is (the same version of) the Monitoring Plugin running there?");
            } else {
                for (String conversationXml : conversationXmls) {
                    try {
                        Log.debug("Interpreting conversation from: {}", conversationXml);
                        final Conversation conversation = Conversation.fromXml(conversationXml);
                        Log.debug("Interpreted conversation: {}", conversation);
                        result.add(conversation);
                    } catch (IOException e) {
                        Log.warn("Conversation could not be reconstructed from '{}' because of '{}'. This conversation is not included in the result set.", conversationXml, e.getMessage());
                    }
                }
            }
            return result;
        }
    }


    public int getArchivedConversationCount() {
        int conversationCount = 0;
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = DbConnectionManager.getConnection();
            pstmt = con.prepareStatement(CONVERSATION_COUNT);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                conversationCount = rs.getInt(1);
            }
        } catch (SQLException sqle) {
            Log.error(sqle.getMessage(), sqle);
        } finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return conversationCount;
    }


    public int getArchivedMessageCount() {
        int messageCount = 0;
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            con = DbConnectionManager.getConnection();
            pstmt = con.prepareStatement(MESSAGE_COUNT);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                messageCount = rs.getInt(1);
            }
        } catch (SQLException sqle) {
            Log.error(sqle.getMessage(), sqle);
        } finally {
            DbConnectionManager.closeConnection(rs, pstmt, con);
        }
        return messageCount;
    }


    public void addConversationListener(ConversationListener listener) {
        conversationListeners.add(listener);
    }

    /**
     * Removes a conversation listener.
     *
     * @param listener
     *            the conversation listener.
     */
    public void removeConversationListener(ConversationListener listener) {
        conversationListeners.remove(listener);
    }


    void processMessage(JID sender, JID receiver, String body, String stanza, Date date) {
        Log.trace("Processing message from date {}...", date );
        String conversationKey = getConversationKey(sender, receiver);
        synchronized (conversationKey.intern()) {
            Conversation conversation = conversations.get(conversationKey);
            // Create a new conversation if necessary.
            if (conversation == null) {
                Collection<JID> participants = new ArrayList<>(2);
                participants.add(sender);
                participants.add(receiver);
                XMPPServer server = XMPPServer.getInstance();
                // Check to see if this is an external conversation; i.e. one of the participants
                // is on a different server. We can use XOR since we know that both JID's can't
                // be external.
                boolean external = isExternal(server, sender) ^ isExternal(server, receiver);
                // Make sure that the user joined the conversation before a message was received
                Date start = new Date(date.getTime() - 1);
                conversation = ConversationDAO.createConversation(this, participants, external, start);
                conversations.put(conversationKey, conversation);
                // Notify listeners of the newly created conversation.
                for (ConversationListener listener : conversationListeners) {
                    listener.conversationCreated(conversation);
                }
            }
            // Check to see if the current conversation exceeds either the max idle time
            // or max conversation time.
            else if ((date.getTime() - conversation.getLastActivity().getTime() > idleTime.toMillis())
                    || (date.getTime() - conversation.getStartDate().getTime() > maxTime.toMillis())) {
                removeConversation(conversationKey, conversation, conversation.getLastActivity());

                Collection<JID> participants = new ArrayList<>(2);
                participants.add(sender);
                participants.add(receiver);
                XMPPServer server = XMPPServer.getInstance();
                // Check to see if this is an external conversation; i.e. one of the participants
                // is on a different server. We can use XOR since we know that both JID's can't
                // be external.
                boolean external = isExternal(server, sender) ^ isExternal(server, receiver);
                // Make sure that the user joined the conversation before a message was received
                Date start = new Date(date.getTime() - 1);
                conversation = ConversationDAO.createConversation(this, participants, external, start);
                conversations.put(conversationKey, conversation);
                // Notify listeners of the newly created conversation.
                for (ConversationListener listener : conversationListeners) {
                    listener.conversationCreated(conversation);
                }
            }
            // Record the newly received message.
            conversation.messageReceived(sender, date);
            if (metadataArchivingEnabled) {
                conversationArchiver.archive(conversation);
            }
            if (messageArchivingEnabled) {
                if (body != null) {
                    /* OF-677 - Workaround to prevent null messages being archived */
                    messageArchiver.archive(new ArchivedMessage(conversation.getConversationID(), sender, receiver, date, body, stanza, false, null) );
                }
            }
            // Notify listeners of the conversation update.
            for (ConversationListener listener : conversationListeners) {
                listener.conversationUpdated(conversation, date);
            }
        }
        Log.trace("Done processing message from date {}.", date );
    }


    void processRoomMessage(JID roomJID, JID sender, JID receiverIfPM, String nickname, String body, String stanza, Date date) {
        Log.trace("Processing room {} message from date {}.", roomJID, date );
        String conversationKey = getRoomConversationKey(roomJID);
        synchronized (conversationKey.intern()) {
            Conversation conversation = conversations.get(conversationKey);
            // Create a new conversation if necessary.
            if (conversation == null) {
                // Make sure that the user joined the conversation before a message was received
                Date start = new Date(date.getTime() - 1);
                conversation = ConversationDAO.createConversation(this, roomJID, false, start);
                conversations.put(conversationKey, conversation);
                // Notify listeners of the newly created conversation.
                for (ConversationListener listener : conversationListeners) {
                    listener.conversationCreated(conversation);
                }
            }
            // Check to see if the current conversation exceeds either the max idle time
            // or max conversation time.
            else if ((date.getTime() - conversation.getLastActivity().getTime() > idleTime.toMillis())
                    || (date.getTime() - conversation.getStartDate().getTime() > maxTime.toMillis())) {
                removeConversation(conversationKey, conversation, conversation.getLastActivity());
                // Make sure that the user joined the conversation before a message was received
                Date start = new Date(date.getTime() - 1);
                conversation = ConversationDAO.createConversation(this, roomJID, false, start);
                conversations.put(conversationKey, conversation);
                // Notify listeners of the newly created conversation.
                for (ConversationListener listener : conversationListeners) {
                    listener.conversationCreated(conversation);
                }
            }
            // Record the newly received message.
            conversation.messageReceived(sender, date);
            if (metadataArchivingEnabled) {
                conversationArchiver.archive( conversation );
            }
            if (roomArchivingEnabled && (roomsArchived.isEmpty() || roomsArchived.contains(roomJID.getNode()))) {
                JID jid = new JID(roomJID + "/" + nickname);
                if (body != null) {
                    /* OF-677 - Workaround to prevent null messages being archived */
                    messageArchiver.archive( new ArchivedMessage(conversation.getConversationID(), sender, jid, date, body, roomArchivingStanzasEnabled ? stanza : "", false, receiverIfPM));
                }
            }
            // Notify listeners of the conversation update.
            for (ConversationListener listener : conversationListeners) {
                listener.conversationUpdated(conversation, date);
            }

            Log.trace("Done processing room {} message from date {}.", roomJID, date );
        }
    }


    void joinedGroupConversation(JID room, JID user, String nickname, Date date) {
        Conversation conversation = getRoomConversation(room);
        if (conversation != null) {
            conversation.participantJoined(this, user, nickname, date.getTime());
        }
    }


    void leftGroupConversation(JID room, JID user, Date date) {
        Conversation conversation = getRoomConversation(room);
        if (conversation != null) {
            conversation.participantLeft(this, user, date.getTime());
        }
    }

    void roomConversationEnded(JID room, Date date) {
        Conversation conversation = getRoomConversation(room);
        if (conversation != null) {
            removeConversation(room.toString(), conversation, date);
        }
    }

    private void removeConversation(String key, Conversation conversation, Date date) {
        conversations.remove(key);
        // Notify conversation that it has ended
        conversation.conversationEnded(this, date);
        // Notify listeners of the conversation ending.
        for (ConversationListener listener : conversationListeners) {
            listener.conversationEnded(conversation);
        }
    }


    private Conversation getRoomConversation(JID room) {
        String conversationKey = room.toString();
        return conversations.get(conversationKey);
    }

    private boolean isExternal(XMPPServer server, JID jid) {
        return !server.isLocal(jid) || gateways.contains(jid.getDomain());
    }


    boolean isConversation(Message message) {
        if (Message.Type.normal == message.getType() || Message.Type.chat == message.getType()) {
            // TODO: how should conversations with components on other servers be handled?
            return isConversationJID(message.getFrom()) && isConversationJID(message.getTo());
        }
        return false;
    }


    private boolean isConversationJID(JID jid) {
        // Ignore conversations when there is no jid
        if (jid == null) {
            return false;
        }
        XMPPServer server = XMPPServer.getInstance();
        if (jid.getNode() == null) {
            return false;
        }

        // Always accept local JIDs or JIDs related to gateways
        // (this filters our components, MUC, pubsub, etc. except gateways).
        if (server.isLocal(jid) || gateways.contains(jid.getDomain())) {
            return true;
        }

        // If not a local JID, always record it.
        if (!jid.getDomain().endsWith(serverInfo.getXMPPDomain())) {
            return true;
        }

        // Otherwise return false.
        return false;
    }


    String getConversationKey(JID jid1, JID jid2) {
        StringBuilder builder = new StringBuilder();
        if (jid1.compareTo(jid2) < 0) {
            builder.append(jid1.toBareJID()).append("_").append(jid2.toBareJID());
        } else {
            builder.append(jid2.toBareJID()).append("_").append(jid1.toBareJID());
        }
        return builder.toString();
    }

    String getRoomConversationKey(JID roomJID) {
        return roomJID.toString();
    }

    public void componentInfoReceived(IQ iq) {
        // Check if the component is a gateway
        boolean gatewayFound = false;
        Element childElement = iq.getChildElement();
        for (Iterator<Element> it = childElement.elementIterator("identity"); it.hasNext();) {
            Element identity = it.next();
            if ("gateway".equals(identity.attributeValue("category"))) {
                gatewayFound = true;
            }
        }
        // If component is a gateway then keep track of the component
        if (gatewayFound) {
            gateways.add(iq.getFrom().getDomain());
        }
    }

    public void componentRegistered(JID componentJID) {
        // Do nothing
    }

    public void componentUnregistered(JID componentJID) {
        // Remove stored information about this component
        gateways.remove(componentJID.getDomain());
    }

    void queueParticipantLeft(Conversation conversation, JID user, ConversationParticipation participation) {
        RoomParticipant updatedParticipant = new RoomParticipant();
        updatedParticipant.conversationID = conversation.getConversationID();
        updatedParticipant.user = user;
        updatedParticipant.joined = participation.getJoined();
        updatedParticipant.left = participation.getLeft();
        participantArchiver.archive( updatedParticipant );
    }


    public static Long getMessageIdForStableId( final JID owner, final String value )
    {
        Log.debug( "Looking for ID of the message with stable/unique stanza ID {}", value );

        Connection connection = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try
        {
            connection = DbConnectionManager.getConnection();
            pstmt = connection.prepareStatement( "SELECT messageId, stanza FROM ofMessageArchive WHERE messageId IS NOT NULL AND (fromJID = ? OR toJID = ?) AND stanza LIKE ? AND stanza LIKE ?" );
            pstmt.setString( 1, owner.toBareJID() );
            pstmt.setString( 2, owner.toBareJID() );
            pstmt.setString( 3, "%"+value+"%" );
            pstmt.setString( 4, "%urn:xmpp:sid:%" ); // only match stanzas if some kind of XEP-0359 namespace is used.

            rs = pstmt.executeQuery();
            while ( rs.next() ) {
                final Long messageId = rs.getLong( "messageId" );
                final String stanza = rs.getString( "stanza" );
                Log.trace( "Iterating over message with ID {}.", messageId );
                try
                {
                    final Document doc = DocumentHelper.parseText( stanza );
                    final Message message = new Message( doc.getRootElement() );
                    final String sid = StanzaIDUtil.findFirstUniqueAndStableStanzaID( message, owner.toBareJID() );
                    if ( sid != null ) {
                        Log.debug( "Found stable/unique stanza ID {} in message with ID {}.", value, messageId );
                        return messageId;
                    }
                }
                catch ( DocumentException e )
                {
                    Log.warn( "An exception occurred while trying to parse stable/unique stanza ID from message with database id {}.", value );
                }
            }
        }
        catch ( SQLException e )
        {
            Log.warn( "An exception occurred while trying to determine the message ID for stanza ID '{}'.", value, e );
        }
        finally
        {
            DbConnectionManager.closeConnection( rs, pstmt, connection );
        }

        Log.debug( "Unable to find ID of the message with stable/unique stanza ID {}", value );

        try {
            Log.debug( "Fallback mechanism: parse value as old database identifier: '{}'", value );
            return Long.parseLong( value );
        } catch ( NumberFormatException e1 ) {
            Log.debug( "Fallback failed: value cannot be parsed as the old database identifier." );
            throw new IllegalArgumentException( "Unable to parse value '" + value + "' as a database identifier." );
        }
    }


    public Duration availabilityETA( Instant instant )
    {
        final Duration localNode = availabilityETAOnLocalNode( instant );
        if ( !localNode.isZero() )
        {
            return localNode;
        }

        // Check all other cluster nodes.
        final Collection<Duration> objects = CacheFactory.doSynchronousClusterTask( new GetConversationsWriteETATask( instant ), false );
        final Duration maxDuration = objects.stream().max( Comparator.naturalOrder() ).orElse( Duration.ZERO );
        return maxDuration;
    }


    public Duration availabilityETAOnLocalNode( Instant instant )
    {
        if ( instant == null )
        {
            throw new IllegalArgumentException( "Argument 'instant' cannot be null." );
        }

        // Each muc-service writes it's own messages.
        final Stream<Archiver> mucService = XMPPServer.getInstance().getMultiUserChatManager().getMultiUserChatServices().stream().map( MultiUserChatService::getArchiver );
        final Stream<Archiver> monitoring = Stream.of( conversationArchiver, messageArchiver, participantArchiver );

        return Stream.concat( mucService, monitoring )
            .map( archiver -> archiver.availabilityETAOnLocalNode( instant ) )
            .max( Comparator.naturalOrder() )
            .orElse( Duration.ZERO );
    }

    public Conversation getConversation(JID from, JID to, Date currentDate) {
        // هنا، أضفت إرجاع قيمة فارغة (null) كمكان مؤقت.
        // قد تحتاج لإضافة الخطوات الحقيقية للحصول على الـ Conversation بناءً على المدخلات.
        return null;
    }



    private static class ConversationArchivingRunnable extends Archiver<Conversation>
    {
        public static SystemProperty<Integer> CONVERSATION_MAX_WORK_QUEUE_SIZE = SystemProperty.Builder.ofType(Integer.class)
            .setKey("conversation.archiver.conversation.max-work-queue-size")
            .setDefaultValue(500)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();

        public static SystemProperty<Duration> CONVERSATION_MAX_PURGE_INTERVAL = SystemProperty.Builder.ofType(Duration.class)
            .setKey("conversation.archiver.conversation.max-purge-interval")
            .setDefaultValue(Duration.ofMillis(1000))
            .setChronoUnit(ChronoUnit.MILLIS)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();

        public static SystemProperty<Duration> CONVERSATION_GRACE_PERIOD = SystemProperty.Builder.ofType(Duration.class)
            .setKey("conversation.archiver.conversation.grace-period")
            .setDefaultValue(Duration.ofMillis(50))
            .setChronoUnit(ChronoUnit.MILLIS)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();

        ConversationArchivingRunnable( String id )
        {
            super( id,
                CONVERSATION_MAX_WORK_QUEUE_SIZE.getValue(),
                CONVERSATION_MAX_PURGE_INTERVAL.getValue(),
                CONVERSATION_GRACE_PERIOD.getValue()
            );
        }

        protected void store( List<Conversation> workQueue )
        {
            if ( workQueue.isEmpty() )
            {
                return;
            }

            Connection con = null;
            PreparedStatement pstmt = null;

            try
            {
                con = DbConnectionManager.getConnection();
                pstmt = con.prepareStatement(UPDATE_CONVERSATION);

                for ( final Conversation work : workQueue )
                {
                    pstmt.setLong( 1, work.getLastActivity().getTime() );
                    pstmt.setInt( 2, work.getMessageCount() );
                    pstmt.setLong( 3, work.getConversationID() );
                    if ( DbConnectionManager.isBatchUpdatesSupported() )
                    {
                        pstmt.addBatch();
                    }
                    else
                    {
                        pstmt.execute();
                    }
                }

                if ( DbConnectionManager.isBatchUpdatesSupported() )
                {
                    pstmt.executeBatch();
                }
            }
            catch ( Exception e )
            {
                Log.error( "Unable to archive conversation data!", e );
            }
            finally
            {
                DbConnectionManager.closeConnection( pstmt, con );
            }
        }
    }

    /**
     * Stores Messages in the database.
     */
    private static class MessageArchivingRunnable extends Archiver<ArchivedMessage>
    {
        public static SystemProperty<Integer> MESSAGE_MAX_WORK_QUEUE_SIZE = SystemProperty.Builder.ofType(Integer.class)
            .setKey("conversation.archiver.message.max-work-queue-size")
            .setDefaultValue(500)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();

        public static SystemProperty<Duration> MESSAGE_MAX_PURGE_INTERVAL = SystemProperty.Builder.ofType(Duration.class)
            .setKey("conversation.archiver.message.max-purge-interval")
            .setDefaultValue(Duration.ofMillis(1000))
            .setChronoUnit(ChronoUnit.MILLIS)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();

        public static SystemProperty<Duration> MESSAGE_GRACE_PERIOD = SystemProperty.Builder.ofType(Duration.class)
            .setKey("conversation.archiver.message.grace-period")
            .setDefaultValue(Duration.ofMillis(50))
            .setChronoUnit(ChronoUnit.MILLIS)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();
            
        MessageArchivingRunnable( String id )
        {
            super( id,
                MESSAGE_MAX_WORK_QUEUE_SIZE.getValue(),
                MESSAGE_MAX_PURGE_INTERVAL.getValue(),
                MESSAGE_GRACE_PERIOD.getValue()
            );
        }

        @Override
        protected void store( List<ArchivedMessage> workQueue )
        {
            if ( workQueue.isEmpty() )
            {
                return;
            }

            Connection con = null;
            PreparedStatement pstmt = null;

            try
            {
                con = DbConnectionManager.getConnection();
                pstmt = con.prepareStatement(INSERT_MESSAGE);

                for ( final ArchivedMessage work : workQueue )
                {
                    pstmt.setLong(1, work.getID());
                    pstmt.setLong(2, work.getConversationID());
                    pstmt.setString(3, work.getFromJID().toBareJID());
                    pstmt.setString(4, work.getFromJID().getResource());
                    pstmt.setString(5, work.getToJID().toBareJID());
                    pstmt.setString(6, work.getToJID().getResource());
                    pstmt.setLong(7, work.getSentDate().getTime());
                    DbConnectionManager.setLargeTextField(pstmt, 8, work.getBody());
                    DbConnectionManager.setLargeTextField(pstmt, 9, work.getStanza());
                    pstmt.setString(10, work.getIsPMforJID() == null ? null : work.getIsPMforJID().toBareJID());

                    if ( DbConnectionManager.isBatchUpdatesSupported() )
                    {
                        pstmt.addBatch();
                    }
                    else
                    {
                        pstmt.execute();
                    }
                }

                if ( DbConnectionManager.isBatchUpdatesSupported() )
                {
                    pstmt.executeBatch();
                }
            }
            catch ( Exception e )
            {
                Log.error( "Unable to archive message data!", e );
            }
            finally
            {
                DbConnectionManager.closeConnection( pstmt, con );
            }
        }
    }

    /**
     * Stores Participants in the database.
     */
    private static class ParticipantArchivingRunnable extends Archiver<RoomParticipant>
    {
        public static SystemProperty<Integer> PARTICIPANT_MAX_WORK_QUEUE_SIZE = SystemProperty.Builder.ofType(Integer.class)
            .setKey("conversation.archiver.participant.max-work-queue-size")
            .setDefaultValue(500)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();

        public static SystemProperty<Duration> PARTICIPANT_MAX_PURGE_INTERVAL = SystemProperty.Builder.ofType(Duration.class)
            .setKey("conversation.archiver.participant.max-purge-interval")
            .setDefaultValue(Duration.ofMillis(1000))
            .setChronoUnit(ChronoUnit.MILLIS)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();

        public static SystemProperty<Duration> PARTICIPANT_GRACE_PERIOD = SystemProperty.Builder.ofType(Duration.class)
            .setKey("conversation.archiver.participant.grace-period")
            .setDefaultValue(Duration.ofMillis(50))
            .setChronoUnit(ChronoUnit.MILLIS)
            .setDynamic(true)
            .setPlugin(MonitoringConstants.PLUGIN_NAME)
            .build();
            
        ParticipantArchivingRunnable( String id )
        {
            super( id,
                PARTICIPANT_MAX_WORK_QUEUE_SIZE.getValue(),
                PARTICIPANT_MAX_PURGE_INTERVAL.getValue(),
                PARTICIPANT_GRACE_PERIOD.getValue()
            );
        }

        protected void store( List<RoomParticipant> workQueue )
        {
            if ( workQueue.isEmpty() )
            {
                return;
            }

            Connection con = null;
            PreparedStatement pstmt = null;

            try
            {
                con = DbConnectionManager.getConnection();
                pstmt = con.prepareStatement( UPDATE_PARTICIPANT );

                for ( final RoomParticipant work : workQueue )
                {
                    pstmt.setLong(1, work.left.getTime());
                    pstmt.setLong(2, work.conversationID);
                    pstmt.setString(3, work.user.toBareJID());
                    pstmt.setString(4, work.user.getResource() == null ? " " : work.user.getResource());
                    pstmt.setLong(5, work.joined.getTime());
                    if ( DbConnectionManager.isBatchUpdatesSupported() )
                    {
                        pstmt.addBatch();
                    }
                    else
                    {
                        pstmt.execute();
                    }
                }

                if ( DbConnectionManager.isBatchUpdatesSupported() )
                {
                    pstmt.executeBatch();
                }
            }
            catch ( Exception e )
            {
                Log.error( "Unable to archive participant data!", e );
            }
            finally
            {
                DbConnectionManager.closeConnection( pstmt, con );
            }
        }
    }


    private class ConversationPropertyListener implements PropertyEventListener {

        public void propertySet(String property, Map<String, Object> params) {
            if (property.equals("conversation.metadataArchiving")) {
                String value = (String) params.get("value");
                metadataArchivingEnabled = Boolean.valueOf(value);
            } else if (property.equals("conversation.messageArchiving")) {
                String value = (String) params.get("value");
                messageArchivingEnabled = Boolean.valueOf(value);
                // Force metadata archiving enabled on if message archiving on.
                if (messageArchivingEnabled) {
                    metadataArchivingEnabled = true;
                }
            } else if (property.equals("conversation.roomArchiving")) {
                String value = (String) params.get("value");
                roomArchivingEnabled = Boolean.valueOf(value);
                // Force metadata archiving enabled on if message archiving on.
                if (roomArchivingEnabled) {
                    metadataArchivingEnabled = true;
                }
            } else if( property.equals( "conversation.roomArchivingStanzas" ) ) {
                String value = (String) params.get( "value" );
                roomArchivingStanzasEnabled = Boolean.valueOf( value );
            } else if (property.equals("conversation.roomsArchived")) {
                String value = (String) params.get("value");
                roomsArchived = StringUtils.stringToCollection(value);
            } else if (property.equals("conversation.idleTime")) {
                Duration value = Duration.ofMinutes(Long.parseLong((String) params.get("value")));
                try {
                    idleTime = value;
                } catch (Exception e) {
                    Log.error(e.getMessage(), e);
                    idleTime = DEFAULT_IDLE_TIME;
                }
            } else if (property.equals("conversation.maxTime")) {
                Duration value = Duration.ofMinutes(Long.parseLong((String) params.get("value")));
                try {
                    maxTime = value;
                } catch (Exception e) {
                    Log.error(e.getMessage(), e);
                    maxTime = DEFAULT_MAX_TIME;
                }
            } else if (property.equals("conversation.maxRetrievable")) {
                Duration value = Duration.ofDays(Long.parseLong((String) params.get("value")));
                try {
                    maxRetrievable = value;
                } catch (Exception e) {
                    Log.error(e.getMessage(), e);
                    maxRetrievable = DEFAULT_MAX_RETRIEVABLE;
                }
            } else if (property.equals("conversation.maxAge")) {
                Duration value = Duration.ofDays(Long.parseLong((String) params.get("value")));
                try {
                    maxAge = value;
                } catch (Exception e) {
                    Log.error(e.getMessage(), e);
                    maxAge = DEFAULT_MAX_AGE;
                }
            } else if (property.equals("conversation.maxTimeDebug")) {
                Duration value = Duration.ofMinutes(Long.parseLong((String) params.get("value")));
                try {
                    Log.info("Monitoring plugin max time overridden (as used by userCreation plugin)");
                    maxTime = value;
                } catch (Exception e) {
                    Log.error(e.getMessage(), e);
                    Log.info("Monitoring plugin max time reset back to " + DEFAULT_MAX_TIME + " minutes");
                    maxTime = DEFAULT_MAX_TIME;
                }
            }
        }

        public void propertyDeleted(String property, Map<String, Object> params) {
            if (property.equals("conversation.metadataArchiving")) {
                setMetadataArchivingEnabled(METADATA_ARCHIVING_ENABLED.getDefaultValue());
            } else if (property.equals("conversation.messageArchiving")) {
                setMessageArchivingEnabled(MESSAGE_ARCHIVING_ENABLED.getDefaultValue());
            } else if (property.equals("conversation.roomArchiving")) {
                setRoomArchivingEnabled(ROOM_ARCHIVING_ENABLED.getDefaultValue());
            } else if (property.equals("conversation.roomArchivingStanzas")) {
                setRoomArchivingStanzasEnabled(ROOM_STANZA_ARCHIVING_ENABLED.getDefaultValue());
            } else if (property.equals("conversation.roomsArchived")) {
                setRoomsArchived(StringUtils.stringToCollection(ROOMS_ARCHIVED.getDefaultValue()));
            } else if (property.equals("conversation.idleTime")) {
                setIdleTime(IDLE_TIME.getDefaultValue());
            } else if (property.equals("conversation.maxTime")) {
                setMaxTime(MAX_TIME.getDefaultValue());
            } else if (property.equals("conversation.maxAge")) {
                setMaxAge(MAX_AGE.getDefaultValue());
            } else if (property.equals("conversation.maxRetrievable")) {
                setMaxRetrievable(MAX_RETRIEVABLE.getDefaultValue());
            }  else if (property.equals("conversation.maxTimeDebug")) {
                Log.info("Monitoring plugin max time reset back to " + DEFAULT_MAX_TIME + " minutes");
                setMaxTime(MAX_TIME.getDefaultValue());
            }
        }

        public void xmlPropertySet(String property, Map<String, Object> params) {
            // Ignore.
        }

        public void xmlPropertyDeleted(String property, Map<String, Object> params) {
            // Ignore.
        }
    }

    private static class RoomParticipant {
        private long conversationID = -1;
        private JID user;
        private Date joined;
        private Date left;
    }
}
