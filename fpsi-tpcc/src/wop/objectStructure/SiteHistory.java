package wop.objectStructure;

import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import wcc.common.RequestId;
import wop.transaction.AbstractObject;
import wop.transaction.RWInfo;


public class SiteHistory {
	private ConcurrentSkipListMap<Integer, AbstractObject> Sitehistory ;
	private ConcurrentSkipListMap<Integer, RWInfo> rwHistoryRO ;

	public SiteHistory(AbstractObject obj, int numNodes) {
		Integer timeStamp = new Integer(0);
		Sitehistory= new ConcurrentSkipListMap<Integer, AbstractObject>();
		Sitehistory.put(timeStamp, obj);
		rwHistoryRO=new ConcurrentSkipListMap<Integer, RWInfo>();
		rwHistoryRO.put(timeStamp, new RWInfo());	
	}



	public void addSitehistory(RequestId reqId, String objId,AbstractObject obj, int sn, int exeId, String line ) {
		Sitehistory.put((Integer)sn, obj);
		rwHistoryRO.put((Integer)sn,  new RWInfo());			
	}

	public void addMainRWInfo(RequestId reqId, String objId,int sn, int exeId, String place ) {
		if(rwHistoryRO.get(sn)==null) {
		}	
		if(rwHistoryRO.get(sn).mainReqs.add(reqId)) {

		}
	}


	public Set<RequestId> getMainRWInfo(String objId, int sn){
		while(rwHistoryRO.get(sn)==null) {}
		return rwHistoryRO.get(sn).mainReqs;
	}

	public boolean addToMainRWINfor(String objId, int sn, RequestId reqId) {
		if(rwHistoryRO.get(sn).mainReqs.add(reqId)) {
			return true;
		}
		else {
			return false;
		}
	}


	public Entry<Integer, AbstractObject> getLatestObject(int view) {
		Entry<Integer, AbstractObject> entry = Sitehistory.floorEntry((Integer)view);
		return entry;
	}
	public int getLatestPossibleSeqNo(int view) {
		Entry<Integer, AbstractObject> entry = Sitehistory.floorEntry((Integer)view);
		return entry.getKey().intValue();
	}

	public AbstractObject getLatestObject() {
		Entry<Integer, AbstractObject> entry = Sitehistory.lastEntry();
		return entry.getValue();		
	}

	public int getLatestSeqNo(	) {
		int key=Sitehistory.lastKey();	
		return key;
	}

}
