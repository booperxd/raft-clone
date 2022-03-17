package lvc.cds.raft;

public class VoteMessage implements Message {
    private int term;
    private String candidateId;
    private boolean grantedVote;
    private String voterId;

    public VoteMessage(int term, String candidateId, boolean grantedVote, String voterId) {
        this.term = term;
        this.candidateId = candidateId;
        this.setGrantedVote(grantedVote);
        this.setVoterId(voterId);
    }

    public String getVoterId() {
        return voterId;
    }
    public void setVoterId(String voterId) {
        this.voterId = voterId;
    }
    public boolean getGrantedVote() {
        return grantedVote;
    }
    public void setGrantedVote(boolean grantedVote) {
        this.grantedVote = grantedVote;
    }
    public int getTerm() {
        return term;
    }
    public String getCandidateId() {
        return candidateId;
    }
    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }
    public void setTerm(int term) {
        this.term = term;
    }
}
