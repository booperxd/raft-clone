package lvc.cds.raft;

import java.util.ArrayList;

public class AppendMessage implements Message {
    private int term;
    private String leaderId;
    private int prevLogIndex;
    private int prevLogTerm;
    private ArrayList<LogEntry> entries;
    private int leaderCommit;
    private boolean appended;
    private String followerId;

    public AppendMessage(int term, String leaderId, int prevLogIndex, int prevLogTerm, ArrayList<LogEntry> entries, int leaderCommit, boolean appended, String followerId) {
        this.setTerm(term);
        this.setLeaderId(leaderId);
        this.setPrevLogIndex(prevLogIndex);
        this.setPrevLogTerm(prevLogTerm);
        this.setEntries(entries);
        this.setLeaderCommit(leaderCommit);
        this.setAppended(appended);
        this.setFollowerId(followerId);
    }

    public String getFollowerId() {
        return followerId;
    }

    public void setFollowerId(String followerId) {
        this.followerId = followerId;
    }

    public boolean getAppended() {
        return appended;
    }

    public void setAppended(boolean appended) {
        this.appended = appended;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public ArrayList<LogEntry> getEntries() {
        return entries;
    }

    public void setEntries(ArrayList<LogEntry> entries) {
        this.entries = entries;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

}
