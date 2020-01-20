package wop.TwoPhaseCommit;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;



public class ReadRequest{
	private ConcurrentHashMap<String, AtomicInteger> readInfo;


	public ReadRequest()	{
		readInfo=new ConcurrentHashMap<String, AtomicInteger>();
	}

	public void AddToIdList(String Id){
		readInfo.put(Id, new AtomicInteger(1));
	}


	public synchronized boolean FisrtResponse(String Id){	
		synchronized(readInfo) {
			if(readInfo.containsKey(Id)) {
				readInfo.remove(Id);
				return true;
			}
			else {
				return false;
			}		
		}	
	}

}