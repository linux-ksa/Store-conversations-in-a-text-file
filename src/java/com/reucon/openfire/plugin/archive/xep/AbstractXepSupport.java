package com.reucon.openfire.plugin.archive.xep;

import org.dom4j.QName;
import org.jivesoftware.openfire.IQRouter;
import org.jivesoftware.openfire.XMPPServer;
import org.jivesoftware.openfire.auth.UnauthorizedException;
import org.jivesoftware.openfire.disco.IQDiscoInfoHandler;
import org.jivesoftware.openfire.disco.ServerFeaturesProvider;
import org.jivesoftware.openfire.disco.UserFeaturesProvider;
import org.jivesoftware.openfire.handler.IQHandler;
import org.jivesoftware.openfire.muc.MultiUserChatManager;
import org.jivesoftware.openfire.muc.MultiUserChatService;
import org.jivesoftware.openfire.plugin.MonitoringPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmpp.packet.IQ;
import org.xmpp.packet.PacketError;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractXepSupport implements UserFeaturesProvider {

    private static final Logger Log = LoggerFactory.getLogger(AbstractXepSupport.class);

    protected final XMPPServer server;
    protected final Map<QName, IQHandler> element2Handlers;
    protected final IQHandler iqDispatcher;
    protected final String namespace;
    protected boolean muc;
    protected Collection<IQHandler> iqHandlers;

    public AbstractXepSupport(XMPPServer server, String namespace,String iqDispatcherNamespace, String iqDispatcherName, boolean muc) {

        this.server = server;
        this.element2Handlers = Collections.synchronizedMap(new HashMap<>());
        this.iqDispatcher = new AbstractIQHandler(iqDispatcherName, null, iqDispatcherNamespace) {
            public IQ handleIQ(IQ packet) throws UnauthorizedException {
                if (!MonitoringPlugin.getInstance().isEnabled()) {
                    return error(packet,
                            PacketError.Condition.feature_not_implemented);
                }

                final IQHandler iqHandler = element2Handlers.get(packet.getChildElement().getQName());
                if (iqHandler != null) {
                    return iqHandler.handleIQ(packet);
                } else {
                    return error(packet,
                            PacketError.Condition.feature_not_implemented);
                }
            }
        };
        this.namespace = namespace;
        this.iqHandlers = Collections.emptyList();
        this.muc = muc;
    }

    public void start() {
        for (IQHandler iqHandler : iqHandlers) {
            try {
                iqHandler.initialize(server);
                iqHandler.start();
            } catch (Exception e) {
                Log.error("Unable to initialize and start "
                        + iqHandler.getClass());
                continue;
            }

            final QName qName = QName.get( iqHandler.getInfo().getName(), iqHandler.getInfo().getNamespace() );
            element2Handlers.put(qName, iqHandler);
            if (iqHandler instanceof ServerFeaturesProvider) {
                for (Iterator<String> i = ((ServerFeaturesProvider) iqHandler)
                        .getFeatures(); i.hasNext();) {
                    server.getIQDiscoInfoHandler().addServerFeature(i.next());
                }
            }
            if (muc) {
                MultiUserChatManager manager = server.getMultiUserChatManager();
                for (MultiUserChatService mucService : manager.getMultiUserChatServices()) {
                    mucService.addIQHandler(iqHandler);
                    mucService.addExtraFeature(namespace);
                }
            }
        }
        IQDiscoInfoHandler iqDiscoInfoHandler = server.getIQDiscoInfoHandler();
        iqDiscoInfoHandler.addServerFeature(namespace);
        iqDiscoInfoHandler.addUserFeaturesProvider(this);
        server.getIQRouter().addHandler(iqDispatcher);
    }

    public void stop() {
        IQRouter iqRouter = server.getIQRouter();
        IQDiscoInfoHandler iqDiscoInfoHandler = server.getIQDiscoInfoHandler();
        iqDiscoInfoHandler.removeServerFeature( namespace );
        iqDiscoInfoHandler.removeUserFeaturesProvider( this );

        for (IQHandler iqHandler : iqHandlers) {
            final QName qName = QName.get( iqHandler.getInfo().getName(), iqHandler.getInfo().getNamespace() );
            element2Handlers.remove(qName);
            try {
                iqHandler.stop();
                iqHandler.destroy();
            } catch (Exception e) {
                Log.warn("Unable to stop and destroy " + iqHandler.getClass());
            }

            if (iqHandler instanceof ServerFeaturesProvider) {
                for (Iterator<String> i = ((ServerFeaturesProvider) iqHandler)
                        .getFeatures(); i.hasNext();) {
                    iqDiscoInfoHandler.removeServerFeature(i.next());
                }
            }
            if (muc) {
                MultiUserChatManager manager = server.getMultiUserChatManager();
                for (MultiUserChatService mucService : manager.getMultiUserChatServices()) {
                    mucService.removeIQHandler(iqHandler);
                    mucService.removeExtraFeature(namespace);
                }
            }
        }
        if (iqRouter != null) {
            iqRouter.removeHandler(iqDispatcher);
        }
    }

    @Override
    public Iterator<String> getFeatures()
    {
        return Collections.singleton( namespace ).iterator();
    }
}
