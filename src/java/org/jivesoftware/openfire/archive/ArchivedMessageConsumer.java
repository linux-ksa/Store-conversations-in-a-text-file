package org.jivesoftware.openfire.archive;

public interface ArchivedMessageConsumer {
    void consumeMessage(ArchivedMessage message);
}
