package lvc.cds.raft;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import lvc.cds.kvs.KVS;

public class Persist {
    private static final String CURRENT_TERM_LOCATION = 
        System.getProperty("user.dir") + "/persistent-data/currentTerm.txt";
    private static final String VOTED_FOR_LOCATION = 
        System.getProperty("user.dir") + "/persistent-data/votedFor.txt";
    private static final String LOG_LOCATION = 
        System.getProperty("user.dir") + "/persistent-data/log.txt";
    private static final String KVS_LOCATION = 
        System.getProperty("user.dir") + "/persistent-data/kvs.txt";

    public static void saveKVS(KVS kvs) throws IOException {
        kvs.save(KVS_LOCATION);
    }    
    public static KVS loadKVS() throws IOException {
        try {
            return KVS.load(KVS_LOCATION);
        } catch (IOException e) {
            return new KVS();
        }
    }
    
    public static void saveCurrentTerm(int curTerm) throws IOException {
        try {
            File file = new File(CURRENT_TERM_LOCATION);
            
            // creates a new file if and only if the file does not already exist
            file.createNewFile();

            // overwrites file
            FileWriter writer = new FileWriter(file, false);

            // write to the file
            writer.write(curTerm + "");
            
            writer.close();
        }
        catch (FileNotFoundException e) {
            throw new FileNotFoundException("Could not find save location.");
        }
        catch (IOException e) {
            throw new IOException("Could not save currentTerm.");
        }
    }
    public static int loadCurrentTerm() throws IOException {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(CURRENT_TERM_LOCATION));

            String term = reader.readLine();

            reader.close();

            // if there is no term, default is 0
            if(term == null) {
                return 0;
            } else {
                return Integer.parseInt(term);
            }
        } catch (IOException e) {
            throw new IOException("Error loading in currentTerm.");
        }
    }

    public static void saveVotedFor(String vote) throws IOException {
        try {
            File file = new File(VOTED_FOR_LOCATION);
            
            // creates a new file if and only if the file does not already exist
            file.createNewFile();

            // overwrites file
            FileWriter writer = new FileWriter(file, false);

            // write to the file
            writer.write(vote);
            
            writer.close();
        }
        catch (FileNotFoundException e) {
            throw new FileNotFoundException("Could not find save location.");
        }
        catch (IOException e) {
            throw new IOException("Could not save votedFor.");
        }
    }
    public static String loadVotedFor() throws IOException {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(VOTED_FOR_LOCATION)));

            String vote = reader.readLine();

            reader.close();

            // if there is no vote, default is empty String
            if(vote == null) {
                return "";
            } else {
                return vote;
            }
        } catch (IOException e) {
            throw new IOException("Error loading in votedFor.");
        }
    }

    public static void saveLog(ArrayList<LogEntry> log) throws IOException {
        try {
            File file = new File(LOG_LOCATION);
            
            // creates a new file if and only if the file does not already exist
            file.createNewFile();

            // overwrites file
            FileWriter writer = new FileWriter(file, false);

            // write to the file
            for(LogEntry entry : log) {
                writer.write(entry.toString() + "\n");
            }
            
            writer.close();
        }
        catch (FileNotFoundException e) {
            throw new FileNotFoundException("Could not find save location.");
        }
        catch (IOException e) {
            throw new IOException("Could not save log entry.");
        }
    }
    public static ArrayList<LogEntry> loadLog() throws IOException {
        var log = new ArrayList<LogEntry>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File(LOG_LOCATION)));

            while(true) {
                String line = reader.readLine();
                if(line == null) {
                    break;
                }
                var data = line.split(",");
                var entry = new LogEntry(Integer.parseInt(data[0]), data[1], data[2], data[3]);
                log.add(entry);
            }

            reader.close();
            return log;
        } catch (IOException e) {
            throw new IOException("Error loading in log.");
        }
    }
}
