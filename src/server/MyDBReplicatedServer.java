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
import java.util.LinkedList;
/**
 * This class should implement your replicated database server. Refer to
 * {@link ReplicatedServer} for a starting point.
 */
public class MyDBReplicatedServer extends MyDBSingleServer {

    protected final String myID;
    protected final MessageNIOTransport<String,String> serverMessenger;

    private long lamport_clock = 0;

    // encoded as <command>|<callback_id>|<clientAddress>|<server_ID>|<message_lamport>
    private HashMap<String, Integer> messages_acks = new HashMap<String, Integer>(); 
    private PriorityQueue<String> queue = new PriorityQueue<>((s1, s2) -> {
        String[] s1_parts = s1.split("\\|");
        String[] s2_parts = s2.split("\\|");

        if(!s1_parts[4].equals(s2_parts[4])) {
            return Long.compare(Long.parseLong(s1_parts[4]), Long.parseLong(s2_parts[4]));
        }
        // return Long.compare(Long.parseLong(s1_parts[3]), Long.parseLong(s2_parts[3]));
        return s1_parts[3].compareTo(s2_parts[3]);
    });

    private HashMap<String, NIOHeader> client_headers = new HashMap<String, NIOHeader>();   // address of client for server to reply to
    private HashMap<String, Integer> client_headers_count = new HashMap<String, Integer>();   // address of client for server to reply to


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
        // log.log(Level.INFO, "Server {0} with long id {1} started on {2}", new Object[]{this
        //         .myID, this.myID, this.clientMessenger.getListeningSocketAddress()});
    }

    public void close() {
        this.serverMessenger.stop();
        super.close();
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
            // log.log(Level.INFO, "{0} receives message {1} from client {2}", new Object[]{this.myID, request});

            String[] cql = request_parts[0].split(" ");
            /* CHECK FOR READ OPERATIONS */
            if((!cql[0].equals("create") && !cql[0].equals("insert") && !cql[0].equals("update") && 
                !cql[0].equals("drop") && !cql[0].equals("truncate")) || this.serverMessenger.getNodeConfig().getNodeIDs().size() < 2) {

                // log.log(Level.INFO, "request {0} is not write for {1}, calling parent", new Object[]{request, cql[0]});
                super.handleMessageFromClient(bytes, header);
                return;
            }

            synchronized (this) {
                // NOTE that hashmap messages_acks does not include <"UPDATE"|"ACK"> or sender lamport clock

                /* INSERT INTO DATA STRUCTURES */
                this.lamport_clock++;
                String messageToBroadcast = request + "|" + clientSource + "|" + this.myID + "|" + this.lamport_clock;

                if(this.client_headers.containsKey(messageToBroadcast)) {
                    this.client_headers_count.put(messageToBroadcast, this.client_headers_count.get(messageToBroadcast) + 1);
                }
                else {
                    this.client_headers.put(messageToBroadcast, header);
                    this.client_headers_count.put(messageToBroadcast, 1);
                }

                messageToBroadcast += "|" + this.lamport_clock + "|UPDATE"; 

                /* BEGIN MULTICAST */
                // multicast UPDATE to all servers including self
                for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
                    try {
                        this.serverMessenger.send(node, messageToBroadcast.getBytes(ReplicatedServer.DEFAULT_ENCODING));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // TODO: process bytes received from servers here
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // log.log(Level.INFO, "{0} received relayed message from {1}", new Object[]{this.myID, header.sndr}); // simply log

        try {
            String message = new String(bytes, ReplicatedServer.DEFAULT_ENCODING);
            String[] message_parts = message.split("\\|");    // array len 6
            // log.log(Level.INFO, "{0} received relayed message from {1}, message is {2}, array is {3}", new Object[]{this.myID, header.sndr, message, message_parts});

            // encoded as <command>|<callback_id>|<clientAddress>|<server_ID>|<server_lamport>, ignore the <sender_lamport>|<"UPDATE"|"ACK"> at end
            final String message_key = message_parts[0] + "|" + message_parts[1] + "|" + message_parts[2] + "|" + message_parts[3] + "|" + message_parts[4];
            // log.log(Level.INFO, "message_parts len is {0}, message_key is {1}", new Object[]{message_parts.length, message_key});

            synchronized(this) {
                long incoming_lamport_clock = Long.parseLong(message_parts[5]);
                this.lamport_clock = Math.max(this.lamport_clock, incoming_lamport_clock) + 1;  // update LC

                if(message_parts[6].equals("ACK")) {
                    if(this.messages_acks.containsKey(message_key)) {
                        this.messages_acks.put(message_key, this.messages_acks.get(message_key) + 1);
                    }
                    else {
                        this.messages_acks.put(message_key, 1);
                    }

                    String contents = "";
                    PriorityQueue<String> temp = new PriorityQueue<String>(this.queue);
                    LinkedList<String> temp2 = new LinkedList<String>();
                    while(!temp.isEmpty()){
                        // contents += temp.poll() + "\n";
                        temp2.add(temp.poll());
                    }
                    temp2.sort((s1, s2) -> {
                        String[] s1_parts = s1.split("\\|");
                        String[] s2_parts = s2.split("\\|");

                        if(!s1_parts[4].equals(s2_parts[4])) {
                            return Long.compare(Long.parseLong(s1_parts[4]), Long.parseLong(s2_parts[4]));
                        }
                        // return Long.compare(Long.parseLong(s1_parts[3]), Long.parseLong(s2_parts[3]));
                        return s1_parts[3].compareTo(s2_parts[3]);
                    });
                    for(String s: temp2) {
                        contents += s + "\n";
                    }                    
                    log.log(Level.INFO, "{0} ACKS message key {1}, its ack count is now {2}, the contents of queue is {3}", new Object[]{this.myID, message_key, this.messages_acks.get(message_key), contents});
                    // log.log(Level.INFO, "{0} ACKS message key {1}, its ack count is now {2}", new Object[]{this.myID, message_key, this.messages_acks.get(message_key)});
                }
                else if(message_parts[6].equals("UPDATE")) {

                    this.queue.add(message_key);

                    this.lamport_clock = this.lamport_clock + 1;
                    String messageToBroadcast = message_key + "|" + this.lamport_clock + "|ACK";    // append |<sender_lamport>|ACK
                    // log.log(Level.INFO, "{0} multicasts message {1}, head of queue is {2}", new Object[]{this.myID, messageToBroadcast, this.queue.peek()});

                    /* BEGIN MULTICAST */
                    // multicast ACK to all servers including itself
                    for (String node : this.serverMessenger.getNodeConfig().getNodeIDs()) {
                        try {
                            this.serverMessenger.send(node, messageToBroadcast.getBytes(ReplicatedServer.DEFAULT_ENCODING));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
                else {
                    // log.log(Level.INFO, "SOMETHING WRONG! ACK/UPDATE NOT PARSED");
                }
                this.checkMessageDelivery();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkMessageDelivery() {
        try{
            synchronized (this) {
                /* HANDLE MESSAGE DELIVERY */
                while (!this.queue.isEmpty()) {

                    String front_message = this.queue.peek();

                    if(!this.messages_acks.containsKey(front_message)) {    // means not ready yet
                        return;
                    }

                    int acks = this.messages_acks.get(front_message);
                    boolean started_TOM = this.client_headers.containsKey(front_message);

                    if(acks >= this.serverMessenger.getNodeConfig().getNodeIDs().size()) {  // Enough acks, lets goooo!!
                        String[] front_message_parts = front_message.split("\\|");
                        
                        this.session.execute(front_message_parts[0]);   // deliver message

                        this.queue.poll();  // remove message
                        if(this.messages_acks.containsKey(front_message)) {
                            this.messages_acks.remove(front_message);
                        }

                        // log.log(Level.INFO, "{0} delivers message {1}", new Object[]{this.myID, front_message});

                        // If this server is responsible for replying to client
                        if(started_TOM) {

                            String response = front_message_parts[0] + "|" + front_message_parts[1];
                            NIOHeader client_header = this.client_headers.get(front_message);

                            if(this.client_headers_count.get(front_message) > 1) {
                                this.client_headers_count.put(front_message, this.client_headers_count.get(front_message) - 1);
                            }
                            else {
                                this.client_headers.remove(front_message);
                                this.client_headers_count.remove(front_message);
                            }

                            // log.log(Level.INFO, "{0} sends message {1} to client {2}", new Object[]{this.myID, response, client_header.sndr});
                            this.clientMessenger.send(client_header.sndr, response.getBytes(ReplicatedServer.DEFAULT_ENCODING));  // echo message
                        }
                    }
                    else {
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}