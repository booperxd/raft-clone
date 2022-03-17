package lvc.cds.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.AppendEntriesMessage;
import lvc.cds.raft.proto.RaftRPCGrpc;
import lvc.cds.raft.proto.RequestVoteMessage;
import lvc.cds.raft.proto.Response;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCStub;
import lvc.cds.kvs.KVS;
import org.json.simple.*;

public class RaftNode {
    
    public enum NODE_STATE {
        FOLLOWER, LEADER, CANDIDATE, SHUTDOWN
    }

    private ConcurrentLinkedQueue<Message> messages;
    private ConcurrentLinkedQueue<String> clientRequests;

    private NODE_STATE state;
    private Server rpcServer;
    private Map<String, PeerStub> peers;
    private boolean peersConnected;
    private static String myIP;
    private static Random random = new Random();
    private static int BASE_ELECTION_TIMEOUT = 2000;
    private static int HEARTBEAT_TIMEOUT = 400;
    private static int MESSAGE_THROTTLE = 65;
    protected int port;

    // Persistant data
    private static int currentTerm;
    private static String votedFor;
    private static ArrayList<LogEntry> log;
    private static KVS kvs;

    // Volatile
    private static int commitIndex = -1;
    private static int lastApplied = -1;

    protected RaftNode(int port, String me, List<String> peers) throws IOException {
        myIP = me;

        // incoming RPC messages come to this port
        this.port = port;
        this.messages = new ConcurrentLinkedQueue<>();
        this.clientRequests = new ConcurrentLinkedQueue<>();

        // a map containing stubs for communicating with each of our peers
        this.peers = new HashMap<>();
        for (var p : peers) {
            if (!p.equals(me))
                this.peers.put(p, new PeerStub(p, port, messages));
        }

        // lazily connect to peers.
        this.peersConnected = false;
        
        this.state = NODE_STATE.FOLLOWER;
    }

    public void run() throws IOException {
        // load in persistent data
        currentTerm = Persist.loadCurrentTerm();
        votedFor = Persist.loadVotedFor();
        log = Persist.loadLog();
        kvs = Persist.loadKVS();
        
        // start listening for incoming messages now, so that others can connect
        startRpcListener();
        
        // note: we defer making any outgoing connections until we need to send
        // a message. This should help with startup.
        // a state machine implementation.
        state = NODE_STATE.FOLLOWER;
        while (state != NODE_STATE.SHUTDOWN) {
            switch (state) {
            case FOLLOWER:
                state = follower();
                break;
            case LEADER:
                state = leader();
                break;
            case CANDIDATE:
                state = candidate();
                break;
            case SHUTDOWN:
                break;
            }
        }

        // shut down the server
        try {
            shutdownRpcListener();
            shutdownRpcStubs();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void connectRpcChannels() {
        for (var peer : peers.values()) {
            peer.connect();
        }
    }

    private void shutdownRpcStubs() {
        try {
            for (var peer : peers.values()) {
                peer.shutdown();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startRpcListener() throws IOException {
        // build and start the server to listen to incoming RPC calls.
        rpcServer = ServerBuilder.forPort(port)
                .addService(new RaftRPC(messages, clientRequests))
                .build()
                .start();
        
        // add this hook to be run at shutdown, to make sure our server
        // is killed and the socket closed.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    shutdownRpcListener();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
    
    private void shutdownRpcListener() throws InterruptedException {
        if (rpcServer != null) {
            rpcServer.shutdownNow().awaitTermination();
        }
    }

    private NODE_STATE follower() throws IOException {
        System.out.println(myIP + " is a follower.");

        // If we haven't connected yet...
        if (!peersConnected) {
            connectRpcChannels();
            peersConnected = true;
        }
        
        // set the election timeout for this election cycle
        var electionTimeout = BASE_ELECTION_TIMEOUT + random.nextInt(1000);
        // set the current time
        var latestUpdatedTime = System.currentTimeMillis();

        // an event loop that processes incoming messages and timeout events
        // according to the raft rules for followers.
        while (true) {
            
            // if we can commit an entry, then do it
            if(commitIndex > lastApplied) {
                lastApplied++;
                commit(log.get(lastApplied));
            }

            Message m = messages.poll();
            if(m != null) {
                if(m instanceof VoteMessage) {
                    var msg = (VoteMessage) m;

                    // since we're in follower mode, we didn't request votes,
                    // or if we crashed before replies, we ignore it
                    // thus, we're processing a request for our vote
                    
                    // if our term is smaller, we need to update it and set votedFor to null
                    if(currentTerm < msg.getTerm()) {
                        currentTerm = msg.getTerm();
                        Persist.saveCurrentTerm(currentTerm);
                        votedFor = "";
                        Persist.saveVotedFor(votedFor);
                    }

                    // did we grant our vote?
                    if(msg.getGrantedVote()) {
                        votedFor = msg.getCandidateId();
                        Persist.saveVotedFor(votedFor);
                    }

                }
                else {
                    var msg = (AppendMessage) m;

                    // if our term is smaller, we need to update it and set votedFor to null
                    if(currentTerm < msg.getTerm()) {
                        currentTerm = msg.getTerm();
                        Persist.saveCurrentTerm(currentTerm);
                        votedFor = "";
                        Persist.saveVotedFor(votedFor);
                    }

                    // if we successfully appended and have a non-empty list of things to append,
                    // then do it
                    if(msg.getEntries() != null && msg.getAppended()) {
                        
                        // first we have to delete any conflicting entries.
                        // we delete all entries beyond the entry in our log at the leader's prevLogIndex.
                        // by the Log Matching Property, the leader's log and ours agree on everything up to prevLogIndex,
                        // and everything after that is in our log but not the leader's, so we delete it
                        for(int i = log.size() - 1; i > msg.getPrevLogIndex(); i--) {
                            log.remove(i);
                        }
                        
                        // now add all the entries
                        for(LogEntry entry : msg.getEntries()) {
                            log.add(entry);
                        }

                        // save our log
                        Persist.saveLog(log);
                    }

                    // check our commit index
                    if(msg.getLeaderCommit() > commitIndex) {
                        commitIndex = Math.min(msg.getLeaderCommit(), log.size() - 1);
                    }
                    
                }
                
                // regardless of the message type, we received something, so reset electionTimeout
                latestUpdatedTime = System.currentTimeMillis();
            }
            
            // if we haven't gotten contacted in a while, break and become a candidate
            if(System.currentTimeMillis() - latestUpdatedTime > electionTimeout) {
                break;
            }
        }

        // only reached via election timeout
        return NODE_STATE.CANDIDATE;
    }

    private NODE_STATE leader() throws IOException {
        System.out.println(myIP + " is the leader.");

        // first thing we do is send a heartbeat to each node to establish power
        for(var peer : peers.values()) {
            // setting entries to null bypasses all other checks in follower
            peer.sendAppendEntries(currentTerm, myIP, log.size() - 1, 
                                (log.size()-1 == -1 ? 0 : log.get(log.size()-1).getTerm()), 
                                null, commitIndex);
        }

        // now let's set up the volatile leader data
        HashMap<String, Integer> nextIndex = new HashMap<>();
        HashMap<String, Integer> matchIndex = new HashMap<>();
        HashMap<String, Long> timesSinceHeartbeat = new HashMap<>();
        for (var peer : peers.keySet()) {
            nextIndex.put(peer, log.size());
            matchIndex.put(peer, -1);
            timesSinceHeartbeat.put(peer, System.currentTimeMillis());
        }

        // processing loop
        while (true) {

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // see if we can commit something
            if(commitIndex > lastApplied) {
                lastApplied++;
                commit(log.get(lastApplied));
            }
            
            // handle any requests from the client
            String request = clientRequests.poll();
            if(request != null) {
                var data = request.split(" ");

                LogEntry newEntry = new LogEntry(currentTerm, data[0], data[1], (data.length == 3 ? data[2] : null));

                log.add(newEntry);
                Persist.saveLog(log);
            }
            
            // handle a message
            Message m = messages.poll();
            if (m != null) {
                if (m instanceof VoteMessage) {
                    var msg = (VoteMessage) m;
                    
                    // if it is a response to us, it doesn't really matter what their vote is
                    // if it's a request, then we have to check what we did
                    // in either case, we check our term, and possibly update ourselves

                    // if our term is smaller, we need to update it and switch to follower
                    if(currentTerm < msg.getTerm()) {
                        currentTerm = msg.getTerm();
                        Persist.saveCurrentTerm(currentTerm);

                        votedFor = (msg.getGrantedVote() ? msg.getCandidateId() : "");
                        Persist.saveVotedFor(votedFor);

                        // break and become follower
                        break;
                    }

                }
                else {
                    var msg = (AppendMessage) m;
                    
                    System.out.println("AppendEntries: leader=" + msg.getLeaderId() + ", follower=" + msg.getFollowerId() + ", success=" + msg.getAppended() + ", type=" + (msg.getEntries()==null ? "heartbeat" : "message"));

                    // if we were the recipient, then we need to check if they're viable,
                    // and if so, revert to follower and put the message back on the queue
                    if(currentTerm < msg.getTerm()) {
                        currentTerm = msg.getTerm();
                        Persist.saveCurrentTerm(currentTerm);
                        votedFor = "";
                        Persist.saveVotedFor(votedFor);

                        // put the message back
                        messages.add(msg);

                        // break and become follower
                        break;
                    }

                    // otherwise, we need to check if our request was met
                    // if, oddly, we got a request from another "leader", we know their term is wrong,
                    // so we need to ignore it
                    if(msg.getLeaderId().equals(myIP) && msg.getEntries() != null) {
                        var peer = msg.getFollowerId();
                        if(msg.getAppended()) {
                            matchIndex.put(peer, nextIndex.get(peer));
                            nextIndex.put(peer, nextIndex.get(peer) + 1);
                        } else {
                            nextIndex.put(peer, nextIndex.get(peer) - 1);
                        }
                    }

                }
            } else {
                System.out.println("no messages");
            }
            
            // check if we need to send any messages, 
            // first by seeing if we need to send a legitimate AppendEntries,
            // then checking if we need to send a heartbeat
            for(var peer : peers.keySet()) {
                if(log.size() - 1 >= nextIndex.get(peer)) {
                    // send all of our entries
                    System.out.println("Sending entries");
                    var entries = (nextIndex.get(peer) <= 0 ? log : new ArrayList<>(log.subList(nextIndex.get(peer), log.size())));
                    peers.get(peer).sendAppendEntries(currentTerm, myIP, nextIndex.get(peer) - 1, 
                                (log.size()-1 == -1 ? 0 : log.get((nextIndex.get(peer)==0 ? 0 : nextIndex.get(peer)-1)).getTerm()), 
                                entries, commitIndex);

                    // reset this peer's timer
                    timesSinceHeartbeat.put(peer, System.currentTimeMillis());
                } else if(System.currentTimeMillis() - timesSinceHeartbeat.get(peer) > HEARTBEAT_TIMEOUT) {
                    // send a heartbeat
                    System.out.println("Sending heartbeat");
                    peers.get(peer).sendAppendEntries(currentTerm, myIP, log.size() - 1, 
                                (log.size()-1 == -1 ? 0 : log.get(log.size()-1).getTerm()), 
                                null, commitIndex);

                    // reset this peer's timer
                    timesSinceHeartbeat.put(peer, System.currentTimeMillis());
                }
            }


            // if there exists an element in our log bigger than our commit index,
            // and that is from our term,
            // then we need to check if a majority of nodes appended it to their logs
            int N = commitIndex + 1;
            if(N < log.size() && log.get(N).getTerm() == currentTerm) {
                
                // if a majority of matchIndices are >= N, then commitIndex++                
                int count = 1;
                int majority = (peers.size()+1)/2 + 1;
                for (var peer : peers.keySet()) {
                    if (matchIndex.get(peer) >= N) {
                        count++;
                    }
                }
                if (count >= majority) {
                    commitIndex++;
                }
            }
        }

        // reached if we get a message with a higher term,
        // if we become follower, we should forget all client requests
        clientRequests.clear();
        return NODE_STATE.FOLLOWER;
    }

    private NODE_STATE candidate() throws IOException {
        System.out.println(myIP + " is a candidate.");

        // increment currentTerm and save it
        currentTerm++;
        Persist.saveCurrentTerm(currentTerm);

        // vote for myself and save it
        votedFor = myIP;
        Persist.saveVotedFor(myIP);
        
        // we need a set of everyone who has voted for us, and a definition of a majority;
        // once the HashSet has a majority of peers (by its size), then we win the election
        HashSet<String> votedForMe = new HashSet<>();
        int majority = (peers.size()+1)/2 + 1;
        votedForMe.add(myIP);

        // set the election timeout for this election cycle
        var electionTimeout = BASE_ELECTION_TIMEOUT + random.nextInt(1000);
        // set the current time
        var latestUpdatedTime = System.currentTimeMillis();

        // send a requestVote to everyone
        for(var peer : peers.values()) {
            peer.sendRequestVote(currentTerm, myIP, log.size()-1, 
                            (log.size()-1 == -1 ? 0 : log.get(log.size()-1).getTerm()));
        }

        // processing loop
        while(true) {
            
            // can we commit something? 
            if(commitIndex > lastApplied) {
                lastApplied++;
                commit(log.get(lastApplied));
            }

            // did we win the election?
            if(votedForMe.size() >= majority) {
                // break out of the while loop and become leader
                break;
            }
            
            // see if we have a message to process
            Message m = messages.poll();
            // handle the message
            if(m != null) {
                if(m instanceof VoteMessage) {
                    var msg = (VoteMessage) m;

                    // did we send the request...
                    if(msg.getCandidateId().equals(myIP)) {
                        
                        // we have to check if our term is out of date, and update it if so
                            // (and convert back to follower)
                        // also, since our term changed, we need to change votedFor
                        if(currentTerm < msg.getTerm()) {
                            currentTerm = msg.getTerm();
                            Persist.saveCurrentTerm(currentTerm);
                            votedFor = "";
                            Persist.saveVotedFor(votedFor);
                            return NODE_STATE.FOLLOWER;
                        }
                        
                        // otherwise, check if we got the vote and tally
                        if(msg.getGrantedVote()) {
                            votedForMe.add(msg.getVoterId());
                        }

                    } // or did we cast a vote?
                    else {
                        
                        // we have to check if our term is out of date, and update it if so
                            // (and convert back to follower)
                        // as well as check if we voted for them
                        if(currentTerm < msg.getTerm()) {
                            currentTerm = msg.getTerm();
                            Persist.saveCurrentTerm(currentTerm);
                            votedFor = (msg.getGrantedVote() ? msg.getCandidateId() : "");
                            Persist.saveVotedFor(votedFor);

                            return NODE_STATE.FOLLOWER;
                        }

                        // if our term is good, then either that other candidate is fake (lower term)
                        // or we're competing for votes
                        // either way, we don't do anything on our end
                        // since we voted for ourselves, and our term is ok, we couldn't have voted for them
                    }

                } // it's an append message
                else {
                    var msg = (AppendMessage) m;

                    // we're not a leader, so there's no chance we sent it, 
                    // otherwise we wouldn't have timed out;
                    // even if we did, just ignore it, we can't do anything about it

                    // if the purported leader is viable, then become a follower
                    // and put the message back in our queue to process as a follower
                    if(currentTerm <= msg.getTerm()) {
                        messages.add(msg);
                        
                        votedFor = (currentTerm < msg.getTerm() ? "" : votedFor);
                        Persist.saveVotedFor(votedFor);

                        currentTerm = msg.getTerm();
                        Persist.saveCurrentTerm(currentTerm);

                        return NODE_STATE.FOLLOWER;
                    }

                }

                // regardless of the message type, we received something, so reset electionTimeout
                latestUpdatedTime = System.currentTimeMillis();
            }

            // see if we timed out, and if so start a new election
            if(System.currentTimeMillis() - latestUpdatedTime > electionTimeout) {
                return NODE_STATE.CANDIDATE;
            }
        }
        
        // only reached if we win the election
        // all other possibilites are in the event loop
        return NODE_STATE.LEADER;
    }

    private static class PeerStub {
        ConcurrentLinkedQueue<Message> messages;
        String peer;
        int port;
        ManagedChannel channel;
        RaftRPCStub stub;
        
        PeerStub(String peer, int port, ConcurrentLinkedQueue<Message> messages) {
            this.peer = peer;
            this.port = port;
            this.channel = null;
            this.stub = null;
            this.messages = messages;
        }

        void connect() {
            channel = ManagedChannelBuilder.forAddress(peer, port).usePlaintext().build();
            stub = RaftRPCGrpc.newStub(channel);
        }

        RaftRPCStub getStub() {
            return stub;
        }

        void shutdown() throws InterruptedException {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }

        void sendAppendEntries(int term, String leaderId, int prevLogIdx, int prevLogTerm, ArrayList<LogEntry> entries, int leaderCommitIndex) {
            
            final boolean heartbeat = entries==null;

            ArrayList<String> values = new ArrayList<>();
            if(entries != null) {
                for (LogEntry entry : entries) {
                    values.add(entry.toString());
                }
            }
            AppendEntriesMessage request = AppendEntriesMessage.newBuilder()
                            .setTerm(term).setLeaderID(leaderId)
                            .setPrevLogIdx(prevLogIdx).setPrevLogTerm(prevLogTerm)
                            .addAllEntries(values)
                            .setLeaderCommitIdx(leaderCommitIndex)
                            .build();

            getStub().appendEntries(request, new StreamObserver<Response>() {
                @Override
                public void onNext(Response value) {
                    // the term is the follower's
                    AppendMessage msg = new AppendMessage(value.getTerm(), myIP, -1, -1, (heartbeat ? null : new ArrayList<LogEntry>()), -1, value.getSuccess(), peer);
                    messages.add(msg);
                }
                @Override
                public void onError(Throwable t) {
                    
                }
                @Override
                public void onCompleted() {
                    
                }
            });
        }

        void sendRequestVote(int term, String candidateId, int lastLogIndex, int lastLogTerm) {
           
            RequestVoteMessage request = RequestVoteMessage.newBuilder()
                            .setTerm(term).setCandidateID(candidateId)
                            .setLastLogIndex(lastLogIndex)
                            .setLastLogTerm(lastLogTerm)
                            .build();
            
            getStub().requestVote(request, new StreamObserver<Response>() {
                @Override
                public void onNext(Response value) {
                    // the term is the follower's
                    VoteMessage msg = new VoteMessage(value.getTerm(), myIP, value.getSuccess(), peer);
                    messages.add(msg);
                }
                @Override
                public void onError(Throwable t) {
                    
                }
                @Override
                public void onCompleted() {
                    
                }
            });
        }
    }
    
    public static int getCurrentTerm() {
        return currentTerm;
    }

    public static String getVotedFor() {
        return votedFor;
    }

    public static ArrayList<LogEntry> getLog() {
        return log;
    }

    public static String getMyIP() {
        return myIP;
    }

    @SuppressWarnings("unchecked")
    public static void commit(LogEntry entry) throws IOException {

        // add?
        if(entry.getVerb().equals("ADD")) {
            JSONObject obj =  new JSONObject();
            obj.put("Term", entry.getTerm());
            obj.put("Value", entry.getValue());

            kvs.add(entry.getKey(), obj);
        } // modify?
        else if(entry.getVerb().equals("MODIFY")) {
            JSONObject obj =  new JSONObject();
            obj.put("Term", entry.getTerm());
            obj.put("Value", entry.getValue());

            kvs.update(entry.getKey(), obj);
        } // or delete? 
        else {
            kvs.remove(entry.getKey());
        }

        // save our kvs
        Persist.saveKVS(kvs);
    }

}      