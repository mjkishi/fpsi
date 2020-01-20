package wop.transaction;

import java.util.HashMap;
import java.util.Map;

public class ReadSet {
    public Map<String, ReadSetEntry> readset;
    
    public ReadSet() {
    	readset = new HashMap<String, ReadSetEntry>();
    }
    
    public void addToReadSet(String objId, ReadSetEntry readsetEntry) {
    	readset.put(objId, readsetEntry);
    }
    
    public Map<String, ReadSetEntry> getReadSet() {
    	return readset;
    }
}