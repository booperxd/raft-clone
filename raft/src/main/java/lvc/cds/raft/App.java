
package lvc.cds.raft;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class App {
    public static final int PORT = 5777;

    public static void main(String[] args) {

        try {
            String me = InetAddress.getLocalHost().getHostAddress();
            System.out.println(me);
            ArrayList<String> peers = new ArrayList<>();
            peers.add("10.2.192.171");
            peers.add("10.1.81.22");
            //peers.add("10.2.201.41");
            //peers.add("10.1.23.53");
            

            RaftNode node = new RaftNode(PORT, me, peers);

            node.run();
            
        } catch (UnknownHostException e) {
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
}
