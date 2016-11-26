import static java.lang.Math.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedList;
import java.io.*;
import java.nio.ByteBuffer; 
import java.nio.channels.ClosedChannelException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

// Assumptions:
/**
 * Still breaking neighbor links for completion and passing along the CR message
 * Assuming that CP and recovery never fails
*/



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

    public void run() {
        long threadId = Thread.currentThread().getId();

        System.out.println("Application running "+threadId);
        System.out.println("d = "+d);
        System.out.println("n_i = "+n_i);
        System.out.println("n_n = "+n_n);

        while(messages > 0) {

            // Delay between send events
            p.delay(p.nextExp(d));

            // Send to a uniformly random neighbor
            int neighbor = (int)Math.floor(Math.random() * n_n);
            p.sendMessage(neighbors[neighbor], MessageType.SIMPLE, false);

            messages = messages - 1;
        }

        System.out.println("Application finished");
        p.sendMessage(0, MessageType.COMPLETE, true);
    }
}

/******************************************************************************/
enum MessageType {
    SIMPLE,             // Application message, do nothing except update clocks...
    COMPLETE,           // This node is complete
    PROTOCOL,           // You're next bro, execute checkpoint or recovery
    CHECKPOINT,         // Command for neighbor to take a checkpoint
    CHECKPOINT_RESP,    // Converge cast Response from neighbor once CP taken
    VECTOR_CLOCK,       
    VECTOR_CLOCK_RESP,
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

    int[] llr;
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

            if(m.type != MessageType.SIMPLE) {
                // System.out.println(p.n_i+" SReceived "+m.type+" from "+m.origin);
            }


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

class ProtocolPasser implements Runnable{

    private Protocol p;
    private int dest;
    private Message m;
    private int delay;

    ProtocolPasser(Protocol p, int dest, Message m, int delay) {
        this.p = p;
        this.dest = dest;
        this.m = m;
        this.delay = delay;
    }

    public void run() {

        p.delay(p.nextExp(delay));

        // Send the protocol message to the next node
        p.sendMessage(dest, m, false);
    }
}

/******************************************************************************/
class Protocol implements Runnable{

    public int n;
    public int n_i;
    private int[] neighbors;
    private String[] hosts;
    private int[] ports;
    private int crDelay;

    private Boolean[] complete;
    private Semaphore sending;

    private volatile int[] clock;
    private volatile int[] llr;          // Last label received
    private volatile int[] fls;          // First label sent
    private volatile int label;

    private Checkpoint tentative;
    private Checkpoint perm;

    private volatile ConcurrentLinkedQueue<Message> receiveQueue;
    private volatile ConcurrentLinkedQueue<Message> tempreceiveQueue;

    public int[][] clockMatrix;
    public Boolean[] vectorReceived;

    Protocol(int n, int n_i, int[] neighbors, String[] hosts, int[] ports, int crDelay) {
        this.n = n;
        this.n_i = n_i;
        this.neighbors= neighbors;
        this.hosts = hosts;
        this.ports = ports;
        this.crDelay = crDelay;

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

        tentative = new Checkpoint();
        tentative.llr = new int[n];
        tentative.fls = new int[n];
        tentative.clock = new int[n];
        perm = new Checkpoint();
        perm.llr = new int[n];
        perm.fls = new int[n];
        perm.clock = new int[n];

        receiveQueue = new ConcurrentLinkedQueue<Message>();  // Server produces messages, protocol consumes
        tempreceiveQueue = new ConcurrentLinkedQueue<Message>();  // Server produces messages, protocol consumes

        clockMatrix = new int[n][n];
        vectorReceived = new Boolean[n];

    }

    public Boolean checkCGS(int[][] clocks) {

        // return true if vector clocks form CGS, false if not

        for(int i=0;i<clocks.length;i++) {
            for(int j = 0;j<clocks.length;j++) {
                if(clocks[i][i] < clocks[i][j])
                    return false;
            }
        }
        return true;
    }

    // Return an exponential random variable from mean lambda
    public double nextExp(int lambda) {
        return (-lambda)*Math.log(1-Math.random())/Math.log(2);
    }

    public void delay(double msec) {

        try {
            Thread.sleep((long)msec);
        } catch(Exception e) {
            e.printStackTrace();
        }
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

    private void updateLLR(int origin, int label, Boolean clear) {
        if(clear) {
            llr[origin] = -1;
        } else {
            llr[origin] = label;
        }
    }

    private void updateFLS(int dest, int label, Boolean clear) {
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
        // System.out.println("Clock:");
        // for(int i = 0; i < n; i++) {
        //     System.out.println(i+": "+clock[i]);
        // }

        // System.out.println("Label: "+label);

        // System.out.println("LLR:");
        // for(int i = 0; i < n; i++) {
        //     System.out.println(i+": "+llr[i]);
        // }

        // System.out.println("FLS:");
        // for(int i = 0; i < n; i++) {
        //     System.out.println(i+": "+fls[i]);
        // }
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

            sc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void sendMessage(int dest, Message m, Boolean broadcast) {

        m.clock = incrementClock(n_i);
        m.label = ++label;

        try {
            sending.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(!broadcast) {
            transmitMessage(m, dest);
            updateFLS(dest, m.label, false);

        } else {
            for(int i = 0; i < n; i++) {
                transmitMessage(m, i);
                updateFLS(i, m.label, false);
            }
        }

        sending.release();
    }

    // Send message to neighbor
    public synchronized void sendMessage(int dest, MessageType type, Boolean broadcast) {

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
            updateFLS(dest, m.label, false);

        } else {
            for(int i = 0; i < n; i++) {
                transmitMessage(m, i);
                updateFLS(i, m.label, false);
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

    public void putQueue(Message m) {
        try {
            receiveQueue.add(m);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startCR(String[] crList, int[] crNodes) {

        Message m = new Message();

        m.type = MessageType.PROTOCOL;
        m.crList = crList;
        m.crNodes = crNodes;
        m.crIndex = 0;

        // Save the destination
        int dest = m.crNodes[m.crIndex];

        // Send the protocol message to the next node
        sendMessage(dest, m, false);
    }

    // -1 if this node is the initiator
    public void crHandler(int origin)
    {

        // TODO: Currently only hanlding CP

        // Empty out the receive queue, assuming that only simple messages will be in the queue
        while(true) {

            // Process messages in the received queue
            if(receiveQueue.peek() != null) {
                Message m = receiveQueue.remove();

                if(m.type == MessageType.SIMPLE) {
                    tempreceiveQueue.add(m);
                    
                } else if(m.type == MessageType.CHECKPOINT) { 
                    // ALRIGHT

                    // Send the CP response
                    Message mr = new Message();
                    mr.type = MessageType.CHECKPOINT_RESP;
                    mr.origin = n_i;

                    transmitMessage(mr, m.origin);
                    System.out.println(n_i+" 0Sent CHECKPOINT_RESP to "+m.origin);


                } else {
                    System.out.println("ERROR: Processed unknown message in CRHandler");
                    System.out.println(m.type+" From "+m.origin);
                    while(true) {}
                }
            } else {
                break;
            }
        }

        // Take tentative checkpoint
        tentative.label = label;

        // Clear our llr and fls
        for(int i = 0; i < n; i++) {

            tentative.llr[i] = llr[i];
            tentative.fls[i] = fls[i];
            tentative.clock[i] = clock[i];

            llr[i] = -1;
            fls[i] = -1;
        }
        
        // Determine which of our neighbors we need to send the message to
        Boolean[] neighborWaiting = new Boolean[n];
        for(int i = 0; i < n; i++) {
            neighborWaiting[i] = false;
        }
        for(int i = 0; i < neighbors.length; i++) {

            // Continue through originator of the CP request
            if(neighbors[i] == origin) {
                continue;
            }

            // This neighbor does not need to take a CP if LLR[i] == bottom
            if(tentative.llr[neighbors[i]] == -1) {
                continue;
            }

            // Send CP request to neighbor
            System.out.println(n_i+" Sending CHECKPOINT to neighbor "+neighbors[i]);

            Message m = new Message();
            m.type = MessageType.CHECKPOINT;
            m.origin = n_i;
            m.clock = tentative.clock;
            m.label = tentative.label;
            m.llr = tentative.llr;

            transmitMessage(m, neighbors[i]);
            neighborWaiting[neighbors[i]] = true;
        }

        int iter = 0;
        
        while(true) {

            // Process messages in the received queue
            if(receiveQueue.peek() != null) {
                Message m = receiveQueue.remove();

                if(m.type == MessageType.CHECKPOINT_RESP) {
                    System.out.println(n_i+" Processing CHECKPOINT_RESP from "+m.origin);
                    neighborWaiting[m.origin] = false;

                } else if(m.type == MessageType.SIMPLE) {
                    tempreceiveQueue.add(m);
                } else if(m.type == MessageType.CHECKPOINT) {
                    // Do nothing, already taking a checkpoint

                        // Send the CP response
                        Message mr = new Message();
                        mr.type = MessageType.CHECKPOINT_RESP;
                        mr.origin = n_i;

                        transmitMessage(mr, m.origin);

                } else {
                    System.out.println("ERROR: Unexpected type received in CRHandler");
                    System.out.println(m.type+" From "+m.origin);
                    while(true) {}
                }
            }

            Boolean stillWaiting = false;
            for(int i = 0; i < n; i++) {
                if(neighborWaiting[i] == true) {
                    stillWaiting = true;
                    break;
                }
            }

            if(stillWaiting == false) {
                break;
            }

            // else if((++iter % 100000000) == 0) {
            //     System.out.println(n_i+" Still waiting "+Thread.currentThread().getId());
            //     for(int i = 0; i < n; i++) {
            //         System.out.println(neighborWaiting[i]);
            //     }
            // }
        }

        // Put the simple messages back in the receive queue
        while(tempreceiveQueue.peek() != null) {
            putQueue(tempreceiveQueue.remove());
        }

        // Commit the checkpoint
        System.out.println(n_i+" Committing checkpoint");
        perm = tentative;
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Protocol running "+threadId);

        int iter = 0;


        while(true) {

            // Process messages in the received queue
            if(receiveQueue.peek() != null) {

                iter = 0;

                Message m = receiveQueue.remove();

                switch(m.type) {
                    case COMPLETE:
                        complete[m.origin] = true;
                    break;
                    case SIMPLE:

                        // System.out.println("Processing SIMPLE from "+m.origin);
                        // Update LLR
                        updateLLR(m.origin, m.label, false);

                        // Uppdate our vector clock
                        mergeClock(m.clock);
                        printClock();
                    break;
                    case PROTOCOL:

                        System.out.println("Node "+n_i+" executing "+m.crList[m.crIndex]);

                        // Lock the sending semaphore to prevent other threads from sending
                        try {
                            sending.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        // Run the CR 
                        crHandler(-1);

                        sending.release();

                        // Clear the vector clock matrix
                        for(int i = 0; i < n; i++) {
                            for(int j = 0; j < n; j++) {
                                clockMatrix[i][j] = 0;
                            }

                            vectorReceived[i] = false;
                        }

                        // Broadcast VECTOR_CLOCK message
                        sendMessage(0, MessageType.VECTOR_CLOCK, true);

                        // Gather the results
                        while(true) {
                            Boolean stillWaiting = false;
                            for(int i = 0; i < n; i++) {
                                if(vectorReceived[i] == true) {
                                    stillWaiting = true;
                                    break;
                                }
                            }

                            if(!stillWaiting) {
                                break;
                            }
                        }

                        // Check in the checkCGS function
                        if(!checkCGS(clockMatrix)) {
                            System.out.println("ERROR, inconsistent global state in last checkpoint");
                            while(true) {}
                        } else {
                            System.out.println(n_i+" Last Checkpoints concurrent");
                        }


                        System.out.println("Node "+n_i+" complete "+m.crList[m.crIndex]+" "+m.crIndex);

                        if(m.crList.length > ++m.crIndex) {

                            // Spawn a new thread to wait and send the protocol message to the next node
                            Thread protocolPasser_thread = new Thread(new ProtocolPasser(this, m.crNodes[m.crIndex], m, crDelay));
                            protocolPasser_thread.start();
                        }

                    break;

                    case CHECKPOINT:

                        System.out.println(n_i+" Processing CHECKPOINT from "+m.origin);


                        // Lock the sending semaphore to prevent other threads from sending
                        try {
                            sending.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        // Determine if we need to take a CP
                        if((m.llr[n_i] >= fls[m.origin]) && (fls[m.origin] > -1)) {
                            // Take a CP
                            crHandler(m.origin);
                        } else {
                            System.out.println(n_i+" Not taking a CP");
                        }

                        // Send the CP response
                        Message mr = new Message();
                        mr.type = MessageType.CHECKPOINT_RESP;
                        mr.origin = n_i;

                        transmitMessage(mr, m.origin);

                        // System.out.println(n_i+" 2Sent CHECKPOINT_RESP to "+m.origin);


                        sending.release();

                    break;

                    case VECTOR_CLOCK:

                        // Send a VECTOR_CLOCK_RESP
                        int dest = m.origin;
                        m.origin = n_i;
                        m.clock = perm.clock;
                        m.type = MessageType.VECTOR_CLOCK_RESP;

                        sendMessage(dest, m, false);

                    break;

                    case VECTOR_CLOCK_RESP:

                        // Store the clock value in the message
                        for(int i = 0; i < n; i++) {
                            clockMatrix[n_i][i] = m.clock[i];
                        }
                        vectorReceived[m.origin] = true;

                    break;

                    default:
                        System.out.println("ERROR: Unexpected "+m.type+" from: "+m.origin);
                        while(true) {}
                }
            } 

            // else if((++iter % 100000000) == 0) {
            //     System.out.println(n_i+" No messages "+Thread.currentThread().getId());
            // }

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
        Protocol prot = new Protocol(n, n_i, neighbors, hostnames, ports, instDelay);
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

        // Wait 5 secodns for the applications to start
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
