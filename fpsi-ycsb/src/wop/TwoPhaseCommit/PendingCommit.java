package wop.TwoPhaseCommit;

import java.util.BitSet;
import java.util.Map;
import java.util.Set;

import wcc.common.RequestId;
import wop.transaction.AbstractObject;

public class PendingCommit implements Comparable<PendingCommit> {
	RequestId reqId;
	Map<String, AbstractObject>WriteSet;
	private int sn;
	private int updator;
	private int[] commitVC;
	private Set<RequestId> collectedSet;
	private BitSet propagationDest;



	public PendingCommit(RequestId reqId,Map<String, AbstractObject>WriteSet, int sn, int updator, int[] commitVC, Set<RequestId> collectedSet){
		this.reqId=reqId;
		this.WriteSet=WriteSet;
		this.sn=sn;
		this.updator=updator;
		this.commitVC=commitVC;
		this.collectedSet=collectedSet;
	}

	
	public PendingCommit(RequestId reqId, int sn, int[] startVC, BitSet propagationDest){
		this.reqId=reqId;
		this.sn=sn;
		this.commitVC=startVC;
		this.propagationDest=propagationDest;
	}

	
	public PendingCommit(RequestId reqId, int sn, int updator){
		this.reqId=reqId;
		this.WriteSet=null;
		this.sn=sn;
		this.updator=updator;
	}

	public Set<RequestId> getCollectedSet(){
		return this.collectedSet;
	}


	public int[] getCommitVC(){

		return this.commitVC;
	}


	public RequestId getRequestId() {
		return this.reqId;
	}

	public Map<String, AbstractObject> getWriteSet(){
		return this.WriteSet;
	}

	public int getSeqNo() {
		return this.sn;
	}


	public int getUpdator() {
		return this.updator;
	}


	public int compareTo(PendingCommit entry) {
		return ((Integer)this.sn).compareTo((Integer) entry.getSeqNo());
	}

	public BitSet getPropagationDestination() {
		return this.propagationDest;
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null || obj.getClass() != this.getClass()) {
			return false;
		}

		PendingCommit pc = (PendingCommit) obj;
		return updator == pc.updator && reqId.equals(pc.reqId);
	}

}
