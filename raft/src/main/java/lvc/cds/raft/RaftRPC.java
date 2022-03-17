package lvc.cds.raft;

import lvc.cds.raft.proto.*;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;
import lvc.cds.raft.proto.RaftRPCGrpc.RaftRPCImplBase;

public class RaftRPC extends RaftRPCImplBase {
    ConcurrentLinkedQueue<Message> messages;
    ConcurrentLinkedQueue<String> clientRequests;


    public RaftRPC(ConcurrentLinkedQueue<Message> messages, ConcurrentLinkedQueue<String> clientRequests) {
        this.messages = messages;
        this.clientRequests = clientRequests;
    }

    @Override
    public void appendEntries(AppendEntriesMessage req, StreamObserver<Response> responseObserver) {

        Response reply;
        ArrayList<LogEntry> entries = new ArrayList<>();
        boolean appended = false;

        int currentTerm = RaftNode.getCurrentTerm();
        var log = RaftNode.getLog();

        // receiver implementation 1
        if(req.getTerm() < currentTerm) {
            // false
            reply = Response.newBuilder().setSuccess(false).setTerm(currentTerm).build();
        }
        // receiver implementation 2 
        else if(log.size() != 0 && (log.size() -1 < req.getPrevLogIdx() || log.get(req.getPrevLogIdx()).getTerm() != req.getPrevLogTerm())) {
            // false
            reply = Response.newBuilder().setSuccess(false).setTerm(currentTerm).build();
        }
        // otherwise, we will add it to the log
        else {
            // true
            reply = Response.newBuilder().setSuccess(true).setTerm(currentTerm).build();
            
            // set up the message
            appended = true;
            for(int i = 0; i < req.getEntriesCount(); i++) {
                String entry = req.getEntries(i);
                String[] data = entry.split(",");
    
                entries.add(new LogEntry(Integer.parseInt(data[0]), data[1], data[2], data[3]));
            }
        }
    
        AppendMessage msg = new AppendMessage(req.getTerm(), req.getLeaderID(), 
                    req.getPrevLogIdx(), req.getPrevLogTerm(), 
                    (entries.size() == 0 ? null : entries), 
                    req.getLeaderCommitIdx(), appended, RaftNode.getMyIP()
                    );

        messages.add(msg);
        
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
    
    @Override
    public void requestVote(RequestVoteMessage req, StreamObserver<Response> responseObserver) {
    
        boolean grantedVote = false;
        int currentTerm = RaftNode.getCurrentTerm();
        Response reply;

        // determine if they get our vote or not
        if(req.getTerm() < currentTerm) {
            reply = Response.newBuilder().setSuccess(false).setTerm(currentTerm).build();
        } else {
            String votedFor = RaftNode.getVotedFor();
            String candidateId = req.getCandidateID();
            var log = RaftNode.getLog();
            
            if((votedFor.equals("") || votedFor.equals(candidateId)) && candidateUpToDate(log, req)) {
                reply = Response.newBuilder().setSuccess(true).setTerm(currentTerm).build();
                grantedVote = true;
            } else {
                reply = Response.newBuilder().setSuccess(false).setTerm(currentTerm).build();
            }
        }

        // put the message in our queue to update ourselves on the next loop
        VoteMessage msg = new VoteMessage(req.getTerm(), req.getCandidateID(), grantedVote, RaftNode.getMyIP());
        messages.add(msg);

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
    
    private static boolean candidateUpToDate(ArrayList<LogEntry> myLog, RequestVoteMessage req) {
        int myLastLogIndex = myLog.size() - 1;
        int myLastLogTerm = (myLastLogIndex == -1 ? 0 : myLog.get(myLastLogIndex).getTerm());

        int candidateLastLogIndex = req.getLastLogIndex();
        int candidateLastLogTerm = req.getLastLogTerm();

        if(candidateLastLogTerm > myLastLogTerm) {
            return true;
        } else if(candidateLastLogTerm == myLastLogTerm) {
            return candidateLastLogIndex >= myLastLogIndex;
        } else {
            return false;
        }
    }
    
    @Override
    public void clientRequest(ClientMessage req, StreamObserver<Empty> responseObserver) {
        clientRequests.add(req.getLog());
    }

}
