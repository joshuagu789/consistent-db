package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * This class should implement the logic necessary to perform the requested
 * operation on the database and return the response back to the client.
 */
public class MyDBSingleServer extends SingleServer {

    protected static Cluster cluster;
    protected static Session session;

    public MyDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);

        session = (cluster = Cluster.builder().addContactPoint(isaDB.getHostName()).build()).connect(keyspace);
    }

    public void close() {
        session.close();
        cluster.close();
        super.close();
        // TODO: cleanly close anything you created here.
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
        // simple echo server
        try {

            String message = "";
            String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
            // System.out.println("handleMessageFromClient received string " + request);

            for(int i = request.length()-1; i >= 0; i--) {
                if(request.charAt(i) == '|') {
                    message = request.substring(0, i);
                    break;
                }
            }

            synchronized (this) {

                // indicates callback id not appended
                if(message == "") {
                    System.out.println("handleMessageFromClient executing string " + request);
                    this.session.execute(request);
                }
                // indicates request contains callback id, so use execute message with id removed
                else {
                    System.out.println("handleMessageFromClient executing string " + message);
                    this.session.execute(message);
                }

                this.clientMessenger.send(header.sndr, bytes);  // echo message
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}