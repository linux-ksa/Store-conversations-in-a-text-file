package org.jivesoftware.openfire.archive;

import org.jivesoftware.openfire.archive.ArchivedMessageConsumer;

import org.xmpp.packet.JID;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class FileArchivedMessageConsumer implements ArchivedMessageConsumer {

    private static final String FILE_PATH = "C:\\test\\file.txt"; // حدد مكان الملف هنا

    @Override
    public void consumeMessage(ArchivedMessage message) {
        JID sender = message.getSender();
        JID receiver = message.getReceiver();
        String body = message.getBody();

        String formattedMessage = String.format("From: %s, To: %s, Message: %s", sender.toString(), receiver.toString(), body);
        saveChatToFile(formattedMessage);
    }

    private void saveChatToFile(String chatMessage) {
        try (FileWriter fw = new FileWriter(FILE_PATH, true);
             BufferedWriter bw = new BufferedWriter(fw);
             PrintWriter out = new PrintWriter(bw)) {
            out.println(chatMessage);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
