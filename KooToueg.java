import static java.lang.Math.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.*;
import java.nio.ByteBuffer; 
import java.nio.channels.ClosedChannelException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

/******************************************************************************/
class Application implements Runnable {

    private int d;              // Delay between send events
    private int n_i;            // The ID of this node
    private int[] neighbors;    // List of neighbors
    private Protocol p;         // Protocol object
    private int messages;

    private int n_n;        // Number of neighbors

    Application(int d, int n_i, int[] neighbors, Protocol p, int messages) {
        this.d = d;
        this.n_i = n_i;
        this.neighbors = neighbors;
        this.p = p;
        this.messages = messages;

        n_n = neighbors.length;
    }

    // Return an exponential random variable from mean lambda
    private double nextExp(int lambda) {
        return (-lambda)*Math.log(1-Math.random())/Math.log(2);
    }

    private void delay(double msec) {

        try {
            Thread.sleep((long)msec);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        long threadId = Thread.currentThread().getId();

        System.out.println("Application running "+threadId);
        System.out.println("d = "+d);
        System.out.println("n_i = "+n_i);
        System.out.println("n_n = "+n_n);

        while(messages > 0) {

            // Delay between send events
            delay(nextExp(d));

            // Send to a uniformly random neighbor
            int neighbor = (int)Math.floor(Math.random() * n_n);
            System.out.println("Sending to neighbor "+neighbors[neighbor]);
            p.sendMessage(neighbors[neighbor], MessageType.SIMPLE, false);

            messages = messages - 1;
        }

        System.out.println("Application finished");
        p.sendMessage(0, MessageType.COMPLETE, true);
    }
}

/******************************************************************************/
enum MessageType {
    SIMPLE,         // Application message, do nothing except update clocks...
    COMPLETE,       // This node is complete
    PROTOCOL,       // You're next bro, execute checkpoint or recovery
}

/******************************************************************************/
class Message implements java.io.Serializable{

    // Variables for the messages being passes
    int[] clock;
    int label;
    int origin;
    MessageType type;

    String [] crList;       // Array or "c" or "r" 
    int [] crNodes;         // Array of nodes to execute protocol
    int crIndex;
}

/******************************************************************************/
class Checkpoint {

    // Variables for the messages being passes
    int[] clock;
    int label;
    int[] llr;
    int[] fls;
}

/******************************************************************************/
class ServerHandler implements Runnable{

    Protocol p;
    SctpChannel sc;

    ServerHandler(Protocol p, SctpChannel sc) {
        this.p = p;
        this.sc = sc;
    }

    public void run() {
        try {
            // Listen for messages from other nodes, pass them to the Protocol class
            byte[] data = new byte[1024];
            ByteBuffer buf = ByteBuffer.wrap(data);

            sc.receive(buf, null, null);

            ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bytesIn);
            Message m = (Message)ois.readObject();
            ois.close();

            // Echo back data to ensure FIFO
            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            sc.send(buf, messageInfo);

            sc.close();

            p.putQueue(m);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/******************************************************************************/
class Server implements Runnable{

    private Protocol p;
    private int port;
    private SctpServerChannel ssc;
    private SctpChannel sc;
    private volatile Boolean closeFlag;

    Server(Protocol p, int port) {
        this.p = p;
        this.port = port;
        closeFlag = false;
    }

    public void closeServer() {
        closeFlag = true;
    }

    public void run() {
        System.out.println("Server running "+port);

        try {
            ssc = SctpServerChannel.open();
            InetSocketAddress serverAddr = new InetSocketAddress(port);
            ssc.bind(serverAddr);            
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Listen for the super thread to close the server
        Thread closeListener = new Thread() {
            public void run(){
                while(true) {
                    if(closeFlag) {
                        try {
                            ssc.close();
                            System.out.println("Closed ssc");
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                        return;
                    }
                }
            }
        };
        closeListener.start();

        while(true) {

            try {

                sc = ssc.accept();

                if(sc == null) {
                    return;
                }

                // Start a ServerHandler thread
                ServerHandler serverHandler = new ServerHandler(p, sc);
                Thread serverHandler_thread = new Thread(serverHandler);
                serverHandler_thread.start();

            
            } catch (Exception e) {
                System.out.println("Closing server");
                return;
            }
        }
    }
}

/******************************************************************************/
class Protocol implements Runnable{

    private int n;
    private int n_i;
    private int[] neighbors;
    private String[] hosts;
    private int[] ports;

    private Boolean[] complete;
    private Semaphore sending;

    private int[] clock;
    private int[] llr;          // Last label received
    private int[] fls;          // First label sent
    private int label;

    private volatile ConcurrentLinkedQueue<Message> receiveQueue;

    Protocol(int n, int n_i, int[] neighbors, String[] hosts, int[] ports) {
        this.n = n;
        this.n_i = n_i;
        this.neighbors= neighbors;
        this.hosts = hosts;
        this.ports = ports;

        complete = new Boolean[n];
        for(int i = 0; i < n; i++) {
            complete[i] = false;
        }
        sending = new Semaphore(1, true);

        // Initialize vector clock
        clock = new int[n];
        for(int i = 0; i < n; i++) {
            clock[i] = 0;
        }

        // Initialize LLR
        llr = new int[n];
        for(int i = 0; i < n; i++) {
            llr[i] = -1;
        }

        // Initialize FLS
        fls = new int[n];
        for(int i = 0; i < n; i++) {
            fls[i] = -1;
        }

        label = -1;

        receiveQueue = new ConcurrentLinkedQueue<Message>();  // Server produces messages, protocol consumes
    }

    // Method to serialize messages
    private byte[] serializeObject(Message m) throws IOException{
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
        oos.writeObject(m);
        oos.flush();
        byte[] bytes = bytesOut.toByteArray();
        bytesOut.close();
        oos.close();
        return bytes;
    }


    // Make sure our operations on the vector clock are synchronized
    private synchronized int[] incrementClock(int i) {
        clock[i] = clock[i] + 1;
        return clock;
    }

    private synchronized void updateLLR(int origin, int label, Boolean clear) {
        if(clear) {
            llr[origin] = -1;
        } else {
            llr[origin] = label;
        }
    }

    private synchronized void updateFLS(int dest, int label, Boolean clear) {
        if(clear) {
            fls[dest] = -1;
        } else if(fls[dest] == -1) {
            fls[dest] = label;
        }
    }

    private synchronized void mergeClock(int[] clock) {
        // Merge the give clock value with our own clock

        for(int i = 0; i < n; i++) {
            if(this.clock[i] < clock[i]) {
                this.clock[i] = clock[i];
            }
        }
    }

    synchronized void printClock() {
        System.out.println("Clock:");
        for(int i = 0; i < n; i++) {
            System.out.println(i+": "+clock[i]);
        }

        System.out.println("Label: "+label);

        System.out.println("LLR:");
        for(int i = 0; i < n; i++) {
            System.out.println(i+": "+llr[i]);
        }

        System.out.println("FLS:");
        for(int i = 0; i < n; i++) {
            System.out.println(i+": "+fls[i]);
        }
    }

    private void transmitMessage(Message m, int dest) {
        try {
            // Send message to dest
            InetSocketAddress serverAddr = new InetSocketAddress(hosts[dest], ports[dest]);
            SctpChannel sc = SctpChannel.open(serverAddr, 0, 0);

            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            sc.send(ByteBuffer.wrap(serializeObject(m)), messageInfo);

            // Wait for echo from server
            byte[] data = new byte[1024];
            ByteBuffer buf = ByteBuffer.wrap(data);

            sc.receive(buf, null, null);

            updateFLS(dest, label, false);

            sc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendMessage(int dest, Message m, Boolean broadcast) {

        m.clock = incrementClock(n_i);
        m.label = ++label;

        try {
            sending.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(!broadcast) {
            transmitMessage(m, dest);
        } else {
            for(int i = 0; i < n; i++) {
                transmitMessage(m, i);
            }
        }

        sending.release();
    }

    // Send message to neighbor
    public void sendMessage(int dest, MessageType type, Boolean broadcast) {

        // Send generic message directly to neighbor
        Message m = new Message();
        m.type = type;
        m.origin = n_i;
        m.clock = incrementClock(n_i);
        m.label = ++label;

        try {
            sending.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(!broadcast) {
            transmitMessage(m, dest);
        } else {
            for(int i = 0; i < n; i++) {
                transmitMessage(m, i);
            }
        }

        sending.release();
    }

    public void broadcastNeighbors(Message m) {
        // Broadcast message to neighbors
        // TODO: make sure we don't send any other messages while we're broadcasting
        // TODO: When flooding, make sure we break cycles by tracking if we've already broadcast
        //       the message
    }

    public synchronized void putQueue(Message m) {
        try {
            receiveQueue.add(m);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startCR(String[] crList, int[] crNodes) {

        System.out.println("Node "+n_i+" executing "+crList[0]);

        // TODO: CR lol

        if(crList.length > 1) {
            // Pass on to next node
            Message m = new Message();

            m.type = MessageType.PROTOCOL;
            m.crList = crList;
            m.crNodes = crNodes;
            m.crIndex = 1;

            // Save the destination
            int dest = m.crNodes[m.crIndex];

            // Send the protocol message to the next node
            sendMessage(dest, m, false);
        }
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Protocol running "+threadId);

        while(true) {

            // Process messages in the received queue
            if(receiveQueue.peek() != null) {
                Message m = receiveQueue.remove();

                System.out.println("Received message from "+m.origin);

                // Update LLR
                updateLLR(m.origin, m.label, false);

                // Uppdate our vector clock
                mergeClock(m.clock);
                printClock();

                switch(m.type) {
                    case COMPLETE:
                        complete[m.origin] = true;
                    break;
                    case SIMPLE:
                    break;
                    case PROTOCOL: 
                        System.out.println("Node "+n_i+" executing "+m.crList[m.crIndex]);

                        // TODO: Do protocol lol
                        // TODO: Spawn another thread to handle the checkpoint/recovery

                        if(m.crList.length > ++m.crIndex) {
                            // Save the destination
                            int dest = m.crNodes[m.crIndex];

                            // Send the protocol message to the next node
                            sendMessage(dest, m, false);
                        }


                    break;
                }
            }

            // Check to see if all of the nodes have completed
            Boolean allComplete = true;
            for(int i = 0; i < n; i++) {
                if(complete[i] == false) {
                    allComplete = false;
                    break;
                }
            }
            if(allComplete) {
                System.out.println("Protocol closing");
                return;
            }
        }
    }
}

/******************************************************************************/
public class KooToueg {

    public static void main(String[] args) {
        System.out.println("*** KooToueg ***");

        // parse the input arguments
        // n n_i cr_n instDelay sendDelay messages hostname[0] port[0] ... neighborSize n[0] ... c r ... 1 3 2 ...

        int n = Integer.parseInt(args[0]);
        int n_i = Integer.parseInt(args[1]);
        int cr_n = Integer.parseInt(args[2]);
        int instDelay = Integer.parseInt(args[3]);
        int sendDelay = Integer.parseInt(args[4]);
        int messages = Integer.parseInt(args[5]);

        System.out.println("n: "+n);
        System.out.println("n_i: "+n_i);
        System.out.println("cr_n: "+cr_n);
        System.out.println("instDelay: "+instDelay);
        System.out.println("sendDelay: "+sendDelay);
        System.out.println("messages: "+messages);

        String[] hostnames = new String[n];
        int[] ports = new int[n];
        System.out.println("Nodes:");
        int i;
        for(i = 0; i < n; i++) {
            hostnames[i] = args[2*i + 6];
            ports[i] = Integer.parseInt(args[2*i + 6 + 1]);

            System.out.println(hostnames[i]+" "+ports[i]);
        }

        int neighborSize = Integer.parseInt(args[2 * i+ 6]);
        System.out.println("neighborSize: "+neighborSize);
        System.out.println("neighbors:");
        int saved_i = 2 * i + 6 + 1;
        int[] neighbors = new int[neighborSize];
        for(i = 0; i < neighborSize; i++) {
            neighbors[i] = Integer.parseInt(args[saved_i+i]);
            System.out.println(neighbors[i]);
        }

        String[] crList = new String[cr_n];
        saved_i=saved_i+i;
        System.out.println("crList:");
        for(i = 0; i < cr_n; i++) {
        	crList[i] = args[saved_i+i];
        	System.out.println(crList[i]);
        }

    	int[] crNodes = new int[cr_n];
        saved_i=saved_i+i;
        System.out.println("crNodes:");
        for(i = 0; i < cr_n; i++) {
        	crNodes[i] = Integer.parseInt(args[saved_i+i]);
        	System.out.println(crNodes[i]);
        }

        // Start the protocol thread
        Protocol prot = new Protocol(n, n_i, neighbors, hostnames, ports);
        Thread protocol_thread = new Thread(prot);
        protocol_thread.start();

        // Start the server thread
        Server server = new Server(prot, ports[n_i]);
        Thread server_thread = new Thread(server);
        server_thread.start();

        // Wait 5 secodns for the applications to start
        try {
            Thread.sleep(5000);
        } catch(Exception e) {
            e.printStackTrace();
        }

        // Start the  application thread
        Thread app_thread = new Thread(new Application(sendDelay, n_i, neighbors, prot, messages));
        app_thread.start();

        // Wait 5 secodns before starting the CR
        try {
            Thread.sleep(5000);
        } catch(Exception e) {
            e.printStackTrace();
        }

        // If this node is first in line for CR, initiate it with the protocol class
        if(crNodes[0] == n_i) {
            prot.startCR(crList, crNodes);
        }

        // Wait for all of the threads to exit
        try {
            app_thread.join();
            protocol_thread.join();
            server.closeServer();
            server_thread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
