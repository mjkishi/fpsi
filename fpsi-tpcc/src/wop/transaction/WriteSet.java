package wop.transaction;

import java.util.HashMap;
import java.util.Map;


public class WriteSet {
	public Map<String, AbstractObject> writeset;

	public WriteSet() {
		writeset = new HashMap<String, AbstractObject>();
	}
	
	public AbstractObject getobject(String objId) {
		return writeset.get(objId);
	}
	
	public void addToWriteSet(String objId, AbstractObject object) {
		writeset.put(objId, object);
	}

	
	public Map<String, AbstractObject> getWriteSet() {
		return writeset;
	}
	

}