package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.logging.Level;

/**
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {

    protected final String myID;
    protected final MessageNIOTransport<String,String> serverMessenger;

    private long lamport_clock = 0;

    // encoded as <command>|<callback_id>|<clientAddress>|<server_ID>|<server_lamport>
    private HashMap<String, Integer> messages_acks = new HashMap<String, Integer>(); 
    private PriorityQueue<String> queue = new PriorityQueue<>((s1, s2) -> {
        String[] s1_parts = s1.split("\\|");
        String[] s2_parts = s2.split("\\|");

        if(s1_parts[4] != s2_parts[4]) {
            return Long.compare(Long.parseLong(s1_parts[4]), Long.parseLong(s2_parts[4]));
        }
        return s1_parts[3].compareTo(s2_parts[3]);
    });
    private HashMap<String, NIOHeader> client_headers = new HashMap<String, NIOHeader>();   // address of client for server to reply to
    // private HashMap<String, byte[]> client_bytes = new HashMap<String, byte[]>();

    public MyDBReplicatedServer(NodeConfig<String> nodeConfig, String myID,
                                InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID)-ReplicatedServer
                        .SERVER_PORT_OFFSET), isaDB, myID);
        this.myID = myID;
        this.serverMessenger = new
                MessageNIOTransport<String, String>(myID, nodeConfig,
                new
                        AbstractBytePacketDemultiplexer() {
                            @Override
                            public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                                handleMessageFromServer(bytes, nioHeader);
                                return true;
                            }
                        }, true);
        log.log(Level.INFO, "Server {0} started on {1}", new Object[]{this
                .myID, this.clientMessenger.getListeningSocketAddress()});
    }

    public void close() {
        super.close();
        this.serverMessenger.stop();
    }

    /**
     *    TODO: process request bytes received from clients here by relaying
     *    them to the database server. The default below simply echoes back
     *    the request.
     *
     *    Extend this method in MySingleServer to implement your logic there.
     *    This file will be overwritten to the original in your submission.
      */
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {

        try {
            String request = new String(bytes, SingleServer.DEFAULT_ENCODING);  // encoded as <command>|<callback_id>
            String clientSource = this.clientMessenger.getListeningSocketAddress().toString();
            String[] request_parts = request.split("\\|");

            if(request_parts.length == 1) { // indicates no encoding of callback
                request += "|-1";   // MyDBClient.java does not use negative id's, basically a dummy id, ensures all message encodings are consistent
            }

            synchronized (this) {
                // encoded as <command>|<callback_id>|<clientAddress>|<server_ID>|<server_lamport>|<"UPDATE"|"ACK">
                // NOTE that hashmap messages_acks does not include <"UPDATE"|"ACK">

                /* INSERT INTO DATA STRUCTURES */
                this.lamport_clock++;
                String messageToBroadcast = request + "|" + clientSource + "|" + this.myID + "|" + this.lamport_clock;

                this.messages_acks.put(messageToBroadcast, 1);   // self ack
                this.queue.add(messageToBroadcast);
                this.client_headers.put(messageToBroadcast, header);

                messageToBroadcast += "|UPDATE"; 

                /* BEGIN MULTICAST */
                // relay to other servers
                for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
                    if (!node.equals(myID))
                        try {
                            this.serverMessenger.send(node, messageToBroadcast.getBytes(ReplicatedServer.DEFAULT_ENCODING));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // TODO: process bytes received from servers here
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        log.log(Level.INFO, "{0} received relayed message from {1}",
                new Object[]{this.myID, header.sndr}); // simply log

        try {
            String message = new String(bytes, ReplicatedServer.DEFAULT_ENCODING);
            String[] message_parts = message.split("\\|");    // array len 6
            log.log(Level.INFO, "{0} received relayed message from {1}, message is {2}, array is {3}", new Object[]{this.myID, header.sndr, message, message_parts});
            // for(int i = 0; i < message_parts.length; i++) {
            //     log.log(Level.INFO, "message at index {0} is {1}", new Object[]{i, message_parts[i]});
            // }
            // encoded as <command>|<callback_id>|<clientAddress>|<server_ID>|<server_lamport>
            final String message_key = message_parts[0] + message_parts[1] + message_parts[2] + message_parts[3] + message_parts[4];
            log.log(Level.INFO, "message_parts len is {0}, message_key is {1}", new Object[]{message_parts.length, message_key});

            synchronized(this) {
                long incoming_lamport_clock = Long.parseLong(message_parts[4]);
                this.lamport_clock = Math.max(this.lamport_clock, incoming_lamport_clock) + 1;  // update LC

                if(message_parts[5].equals("ACK")) {
                    this.messages_acks.put(message_key, this.messages_acks.get(message_key) + 1);
                    log.log(Level.INFO, "{0} ACKS message {1}, its ack count is now {2}", new Object[]{this.myID, message, this.messages_acks.get(message_key)});
                }
                else if(message_parts[5].equals("UPDATE")) {
                    this.messages_acks.put(message_key, 1); // self ack

                    String messageToBroadcast = message_key + "|ACK";
                    log.log(Level.INFO, "{0} multicasts message {1}", new Object[]{this.myID, messageToBroadcast});

                    /* BEGIN MULTICAST */
                    // relay to other servers
                    for (String node : this.serverMessenger.getNodeConfig().getNodeIDs())
                        if (!node.equals(myID))
                            try {
                                this.serverMessenger.send(node, messageToBroadcast.getBytes(ReplicatedServer.DEFAULT_ENCODING));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                }
                else {
                    log.log(Level.INFO, "SOMETHING WRONG! ACK/UPDATE NOT PARSED");
                }

                /* HANDLE MESSAGE DELIVERY */
                // while (!this.queue.isEmpty()) {
                if (!this.queue.isEmpty()) {

                    String front_message = this.queue.peek();
                    int acks = this.messages_acks.get(front_message);

                    if(acks >= this.serverMessenger.getNodeConfig().getNodeIDs().size()) {  // Enough acks, lets goooo!!
                        String[] front_message_parts = front_message.split("\\|");
                        
                        this.session.execute(front_message_parts[0]);   // deliver message

                        this.queue.poll();  // remove message
                        this.messages_acks.remove(front_message);

                        log.log(Level.INFO, "{0} delivers message {1}", new Object[]{this.myID, front_message});

                        // If this server is responsible for replying to client
                        if(this.client_headers.containsKey(front_message)) {

                            String response = front_message_parts[0] + "|" + front_message_parts[1];
                            NIOHeader client_header = this.client_headers.get(front_message);

                            log.log(Level.INFO, "{0} sends message {1} to client {2}", new Object[]{this.myID, response, client_header.sndr});
                            this.clientMessenger.send(client_header.sndr, response.getBytes(ReplicatedServer.DEFAULT_ENCODING));  // echo message
                        }
                    }
                    else {
                        // break;
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}