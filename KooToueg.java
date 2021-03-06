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

        Message m = new Message();
        m.type = MessageType.COMPLETE;
        p.sendMessage(0, m, true);
    }
}

/******************************************************************************/
enum MessageType {
    SIMPLE,             // Application message, do nothing except update clocks...
    COMPLETE,           // This node is complete
    PROTOCOL,           // You're next bro, execute checkpoint or recovery
    CHECKPOINT,         // Command for neighbor to take a checkpoint
    CHECKPOINT_RESP,    // Converge cast Response from neighbor once CP taken
    RECOVERY,           // Command for neighbor to start Recovery
    RECOVERY_RESP,      // Converge cast Response from neighbor once recovery is complete
    VECTOR_CLOCK,       
    VECTOR_CLOCK_RESP,
    VECTOR_CLOCK_CHECK,       
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
    int[] lls;
}

/******************************************************************************/
class Checkpoint {

    // Variables for the messages being passes
    int[] clock;
    int label;
    int[] llr;
    int[] fls;
    int[] lls;
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
class ProtocolPasser implements Runnable{

    private Protocol p;
    private int dest;
    private String[] crList;
    private int[] crNodes;
    private int crIndex;
    private int delay;

    ProtocolPasser(Protocol p, int dest, String [] crList, int [] crNodes, int crIndex, int delay) {
        this.p = p;
        this.dest = dest;
        this.crList = crList;
        this.crNodes = crNodes;
        this.crIndex = crIndex;
        this.delay = delay;
    }

    public void run() {

        p.delay(p.nextExp(delay));

        Message m = new Message();
        m.crList = crList;
        m.crNodes = crNodes;
        m.crIndex = crIndex;
        m.origin = p.n_i;
        m.type = MessageType.PROTOCOL;

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
    private volatile int[] lls;          // Last label sent
    private volatile int[] templls;      // Last label sent, temporary before taking CP
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

        // Initialize LLS
        lls = new int[n];
        for(int i = 0; i < n; i++) {
            lls[i] = -1;
        }

        // Initialize temporary LLS
        templls = new int[n];
        for(int i = 0; i < n; i++) {
            templls[i] = -1;
        }

        label = -1;

        // Initialize checkpoints
        tentative = new Checkpoint();
        tentative.llr = new int[n];
        tentative.fls = new int[n];
        tentative.lls = new int[n];
        tentative.clock = new int[n];

        perm = new Checkpoint();
        perm.llr = new int[n];
        perm.fls = new int[n];
        perm.lls = new int[n];
        perm.clock = new int[n];

        receiveQueue = new ConcurrentLinkedQueue<Message>();  // Server produces messages, protocol consumes
        tempreceiveQueue = new ConcurrentLinkedQueue<Message>();  // Server produces messages, protocol consumes

        clockMatrix = new int[n][n];
        vectorReceived = new Boolean[n];
    }

    public Boolean checkCGS(int[][] clocks) {

        // return true if vector clocks form CGS, false if not
        for(int i=0;i<clocks.length;i++) {
            for(int j = 0;j < clocks.length;j++) {
                if(clocks[i][i] < clocks[j][i]) {

                    System.out.println("Matrix clock:");
                    System.out.println(Arrays.deepToString(clocks));
                    System.out.println(Arrays.toString(clock));

                    System.out.println("i = "+i+"j = "+j);
                    return false;
                }
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

    private void updateTempLLS(int dest, int label) {
        templls[dest] = label;
    }

    private void commitCheckpoint() {

        // Commit the temporary checkpoint
        perm.label = tentative.label;
        for(int i = 0; i < n; i++) {
            perm.clock[i] = tentative.clock[i];
            perm.llr[i] = tentative.llr[i];
            perm.fls[i] = tentative.fls[i];
        }

        // Commit the LLS
        for(int i = 0; i < n; i++) {
            lls[i] = tentative.lls[i];
            templls[i] = -1;
        }

        System.out.println("Checkpoint:");
        System.out.println(Arrays.toString(perm.clock));
    }

    private void rollback() {
        label = perm.label;

        for(int i = 0; i < n; i++) {
            llr[i] = -1;
            fls[i] = -1;
            templls[i] = -1;
            clock[i] = perm.clock[i];

        }

        System.out.println("Recovery:");
        System.out.println(Arrays.toString(clock));
    }

    private synchronized void mergeClock(int[] clock) {
        // Merge the give clock value with our own clock

        for(int i = 0; i < n; i++) {
            if(this.clock[i] < clock[i]) {
                this.clock[i] = clock[i];
            }
        }
        incrementClock(n_i);
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

        try {
            sending.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        m.origin = n_i;

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
    public synchronized void sendMessage(int dest, MessageType type, Boolean broadcast) {

        try {
            sending.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Send generic message directly to neighbor
        Message m = new Message();
        m.type = type;
        m.origin = n_i;
        m.clock = incrementClock(n_i);
        m.label = ++label;

        if(!broadcast) {
            transmitMessage(m, dest);
            updateFLS(dest, m.label, false);
            updateTempLLS(dest, m.label);

        } else {
            for(int i = 0; i < n; i++) {
                transmitMessage(m, i);
                updateFLS(i, m.label, false);
                updateTempLLS(i, m.label);
            }
        }

        sending.release();
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
        m.origin = n_i;

        // Save the destination
        int dest = m.crNodes[m.crIndex];

        // Send the protocol message to the next node
        sendMessage(dest, m, false);
    }

    // -1 if this node is the initiator
    public void checkHandler(int origin)
    {
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
                    // System.out.println(n_i+" 0Sent CHECKPOINT_RESP to "+m.origin);


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
            tentative.lls[i] = lls[i];
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
            // System.out.println(n_i+" Sending CHECKPOINT to neighbor "+neighbors[i]);

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
                    // System.out.println(n_i+" Processing CHECKPOINT_RESP from "+m.origin);
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
        }

        // Put the simple messages back in the receive queue
        while(tempreceiveQueue.peek() != null) {
            putQueue(tempreceiveQueue.remove());
        }

        // Commit the checkpoint
        System.out.println(n_i+" Committing checkpoint");
        commitCheckpoint();
    }

    // -1 if this node is the initiator
    public void recoveryHandler(int origin)
    {
        // Empty out the receive queue, assuming that only simple messages will be in the queue
        while(true) {

            // Process messages in the received queue
            if(receiveQueue.peek() != null) {
                Message m = receiveQueue.remove();

                if(m.type == MessageType.RECOVERY) { 
                    // ALRIGHT

                    // Send the RECOVERY response
                    Message mr = new Message();
                    mr.type = MessageType.RECOVERY_RESP;
                    mr.origin = n_i;

                    transmitMessage(mr, m.origin);
                    //System.out.println(n_i+" 0Sent RECOVERY_RESP to "+m.origin);
                }
            } else {
                break;
            }
        }
        
        // Determine which of our neighbors we need to send the message to
        Boolean[] neighborWaiting = new Boolean[n];
        for(int i = 0; i < n; i++) {
            neighborWaiting[i] = false;
        }
        for(int i = 0; i < neighbors.length; i++) {

            // Continue through originator of the recovery request
            if(neighbors[i] == origin) {
                continue;
            }

            Message m = new Message();
            m.type = MessageType.RECOVERY;
            m.origin = n_i;
            m.clock = clock;
            m.label = label;
            m.lls = lls;

            transmitMessage(m, neighbors[i]);
            neighborWaiting[neighbors[i]] = true;
        }

        int iter = 0;
        
        while(true) {

            // Process messages in the received queue
            if(receiveQueue.peek() != null) {
                Message m = receiveQueue.remove();

                if(m.type == MessageType.RECOVERY_RESP) {
                    // System.out.println(n_i+" Processing RECOVERY_RESP from "+m.origin);
                    neighborWaiting[m.origin] = false;

                } else if(m.type == MessageType.RECOVERY) {
                    // Do nothing, already taking a checkpoint

                    // Send the RECOVERY response
                    Message mr = new Message();
                    mr.type = MessageType.RECOVERY_RESP;
                    mr.origin = n_i;

                    transmitMessage(mr, m.origin);
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
        }

        // Rollback if we did not initiate
        if(origin != -1) {
            System.out.println(n_i+" Rolling back");                
            rollback();
        }
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Protocol running "+threadId);

        while(true) {

            // Process messages in the received queue
            if(receiveQueue.peek() != null) {
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
                    break;
                    case PROTOCOL:

                        System.out.println("Node "+n_i+" executing "+m.crList[m.crIndex]);

                        // Lock the sending semaphore to prevent other threads from sending
                        try {
                            sending.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        if(m.crList[m.crIndex].equals("c")) {
                            checkHandler(-1);                            
                        } else {
                            rollback();
                            recoveryHandler(-1);
                        }

                        // Clear the vector clock matrix
                        for(int i = 0; i < n; i++) {
                            for(int j = 0; j < n; j++) {
                                clockMatrix[i][j] = 0;
                            }

                            vectorReceived[i] = false;
                        }

                        // Insert our own clock into the matrix
                        for(int i = 0; i < n; i++) {

                            if(m.crList[m.crIndex].equals("c")) {
                                clockMatrix[n_i][i] = perm.clock[i];                           
                            } else {
                                clockMatrix[n_i][i] = clock[i];
                            }
                        }
                        vectorReceived[n_i] = true;

                        // Broadcast VECTOR_CLOCK message
                        Message mz = new Message();

                        if(m.crList[m.crIndex].equals("c")) {
                            mz.type = MessageType.VECTOR_CLOCK_CHECK;                           
                        } else {
                            mz.type = MessageType.VECTOR_CLOCK;
                        }

                        mz.origin = n_i;
                        for(int i = 0; i < n; i++) {
                            if(i == n_i) {
                                continue;
                            }
                            transmitMessage(mz, i);                            
                        }

                        // Gather the results
                        while(true) {

                            // Parse the incomming messages
                            if(receiveQueue.peek() != null) {
                                Message mt = receiveQueue.remove();
                                switch(mt.type) {
                                    case VECTOR_CLOCK_RESP:

                                        // Store the clock value in the message
                                        for(int i = 0; i < n; i++) {
                                            clockMatrix[mt.origin][i] = mt.clock[i];
                                        }
                                        vectorReceived[mt.origin] = true;

                                    break;
                                    default:
                                        // tempreceiveQueue.add(mt);
                                }
                            }

                            Boolean stillWaiting = false;
                            for(int i = 0; i < n; i++) {
                                if(vectorReceived[i] == false) {
                                    stillWaiting = true;
                                    break;
                                }
                            }

                            if(!stillWaiting) {
                                break;
                            }
                        }
                        // Put messages back in the receive queue
                        while(tempreceiveQueue.peek() != null) {
                            // putQueue(tempreceiveQueue.remove());
                        }

                        // Check in the checkCGS function
                        if(!checkCGS(clockMatrix)) {
                            System.out.println("ERROR, inconsistent global state");
                            while(true) {}
                        } else {
                            System.out.println(n_i+" Vector clocks concurrent");
                        }

                        System.out.println("Node "+n_i+" complete "+m.crList[m.crIndex]+" "+m.crIndex);

                        if(m.crList.length > ++m.crIndex) {

                            System.out.println("Passing protocol message");

                            // Spawn a new thread to wait and send the protocol message to the next node
                            // ProtocolPasser(Protocol p, int dest, String [] crList, int [] crNodes, int crIndex, int delay) {
                            Thread protocolPasser_thread = new Thread(new ProtocolPasser(this, m.crNodes[m.crIndex], m.crList, m.crNodes, m.crIndex, crDelay));
                            protocolPasser_thread.start();
                        }

                        sending.release();

                    break;

                    case CHECKPOINT:

                        // System.out.println(n_i+" Processing CHECKPOINT from "+m.origin);

                        // Lock the sending semaphore to prevent other threads from sending
                        try {
                            sending.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        // Determine if we need to take a CP
                        if((m.llr[n_i] >= fls[m.origin]) && (fls[m.origin] > -1)) {
                            // Take a CP
                            checkHandler(m.origin);
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

                    case RECOVERY:

                        // System.out.println(n_i+" Processing RECOVERY from "+m.origin);

                        // Lock the sending semaphore to prevent other threads from sending
                        try {
                            sending.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        // Determine if we need to rollback
                        Boolean recovering = false;
                        if((llr[m.origin] > m.lls[n_i])) {
                            // Rollback
                            recovering = true;
                            recoveryHandler(m.origin);
                        } else {
                            System.out.println(n_i+" Not Rolling back");
                        }

                        // Send the RECOVERY_RESP response
                        Message mt = new Message();
                        mt.type = MessageType.RECOVERY_RESP;
                        mt.origin = n_i;

                        transmitMessage(mt, m.origin);

                        if(recovering) {
                            // Wait for vector clock message from initiator
                            while(true) {

                                // Process messages in the received queue
                                if(receiveQueue.peek() != null) {
                                    Message mp = receiveQueue.remove();
                                    if(mp.type == MessageType.RECOVERY) { 
                                        // ALRIGHT

                                        // Send the RECOVERY response
                                        Message mx = new Message();
                                        mx.type = MessageType.RECOVERY_RESP;
                                        mx.origin = n_i;

                                        transmitMessage(mx, mp.origin);
                                        // System.out.println(n_i+" 0Sent RECOVERY_RESP to "+mp.origin);


                                    } else if(mp.type == MessageType.VECTOR_CLOCK) {

                                        // Send a VECTOR_CLOCK_RESP
                                        int dest = mp.origin;
                                        mp.origin = n_i;
                                        mp.clock = clock;
                                        mp.type = MessageType.VECTOR_CLOCK_RESP;

                                        transmitMessage(mp, dest);

                                        break;
                                    }
                                }
                            }
                        }
                        
                        // Unfreeze
                        sending.release(); 

                    break;

                    case VECTOR_CLOCK:

                        // Send a VECTOR_CLOCK_RESP
                        int dest = m.origin;
                        m.origin = n_i;
                        m.clock = clock;
                        m.type = MessageType.VECTOR_CLOCK_RESP;

                        // Lock the sending semaphore to prevent other threads from sending
                        try {
                            sending.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        transmitMessage(m, dest);

                        sending.release(); 

                    break;

                    case VECTOR_CLOCK_CHECK:

                        // Send a VECTOR_CLOCK_RESP
                        dest = m.origin;
                        m.origin = n_i;
                        m.clock = perm.clock;
                        m.type = MessageType.VECTOR_CLOCK_RESP;

                        // Lock the sending semaphore to prevent other threads from sending
                        try {
                            sending.acquire();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        transmitMessage(m, dest);

                        sending.release(); 

                    break;

                    default:
                        System.out.println("ERROR: Unexpected "+m.type+" from: "+m.origin);
                        while(true) {}
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
