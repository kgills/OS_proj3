
/******************************************************************************/
public class KooToueg {

    public static void main(String[] args) {
        System.out.println("*** KooToueg ***");

        // parse the input arguments
        // n cr_n instDelay sendDelay messages hostname[0] port[0] ... neighborSize n[0] ... c r ... 1 3 2 ...

        int n = Integer.parseInt(args[0]);
        int cr_n = Integer.parseInt(args[1]);
        int instDelay = Integer.parseInt(args[2]);
        int sendDelay = Integer.parseInt(args[3]);
        int messages = Integer.parseInt(args[4]);

        System.out.println("n: "+n);
        System.out.println("cr_n: "+cr_n);
        System.out.println("instDelay: "+instDelay);
        System.out.println("sendDelay: "+sendDelay);
        System.out.println("messages: "+messages);

        String[] hostnames = new String[n];
        int[] ports = new int[n];
        System.out.println("Nodes:");
        int i;
        for(i = 0; i < n; i++) {
            hostnames[i] = args[2*i + 5];
            ports[i] = Integer.parseInt(args[2*i + 5 + 1]);

            System.out.println(hostnames[i]+" "+ports[i]);
        }

        int neighborSize = Integer.parseInt(args[2*i+5]);
        System.out.println("neighborSize: "+neighborSize);
        System.out.println("neighbors:");
        int saved_i = 2*i+5+1;
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
    }
}
