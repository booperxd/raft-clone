package lvc.cds.raft;

public class LogEntry {
    private int term;
    private String verb;
    private String key;
    private String value;

    public LogEntry(int t, String ve, String k, String va) {
        setTerm(t);
        setVerb(ve);
        setKey(k);
        setValue(va);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getVerb() {
        return verb;
    }

    public void setVerb(String verb) {
        this.verb = verb;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String toString() {
        return term + "," + verb + "," + key + "," + value;
    }
}
