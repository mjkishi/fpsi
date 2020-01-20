package wop.objectStructure;

import java.util.concurrent.atomic.AtomicInteger;

import wcc.Version;
import wcc.common.RequestId;
import wop.transaction.AbstractObject;




public class ObjectHistory {

	public SiteHistory[] Objecthistory;
	public Version lastversion;
	public int OwnerSite;
	public AtomicInteger lock;
	public int numNodes;


	public  ObjectHistory(int numNodes) {
		Objecthistory = new SiteHistory[numNodes];
		this.lock=new AtomicInteger(-1); 
		this.OwnerSite=-1;
		this.numNodes=numNodes;
	}


	public void registerToH(AbstractObject obj, int numReplicas){
		for(int i=0; i<numReplicas; i++){
			Objecthistory[i]=new SiteHistory(obj,numReplicas);	
			this.lastversion=new Version(obj,numReplicas);
		}
	}

	public boolean LockHistory(String Id, int SiteId, RequestId requestId){
		if( lock.compareAndSet(-1,SiteId*numNodes+requestId.getClientId().intValue()+requestId.getSeqNumber())) {
			return true;
		}
		else {

			return false;
		}
	}


	public void UnLockedbyLocker(int unlockerSite, RequestId requestId, String Id, String place){
		if(!lock.compareAndSet(unlockerSite*numNodes+requestId.getClientId().intValue()+requestId.getSeqNumber(),-1)) {
			System.exit(-1);
		}
		else {
		}
	}


	public int getLockValue(){
		return lock.get();
	}


	public void addObjecthistory(RequestId reqId,String objId, AbstractObject obj, int sn, int ExeId/*, int[] vc*/, String line){
		obj.setVersion(getLatestVersion().getLatestVersionNum()+1);
		Objecthistory[ExeId].addSitehistory(reqId,objId,obj, sn, ExeId,line);
		synchronized(lastversion) {
			lastversion.SetlocalId(ExeId);
			lastversion.SetSeqNo(sn);
			lastversion.setObjec(obj);
		}
	}


	public Version getLatestVersion(){
		synchronized(lastversion) {
			return this.lastversion;
		}
	}

	public int getLastestSeqOfObject(){
		synchronized(lastversion) {
			return lastversion.getSeqNo();
		}
	}

	public int getLatestUpdator(){
		synchronized(lastversion) {
			return lastversion.getlocalId();
		}
	}

	public AbstractObject getObject(int view, int localId){
		return Objecthistory[localId].getLatestObject(view).getValue();
	}


	public AbstractObject getObject(int localId){
		return Objecthistory[localId].getLatestObject();
	}
	public  SiteHistory getSitehistory(int localId){
		return Objecthistory[localId];

	}

}
