package wop.TwoPhaseCommit;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import wcc.common.RequestId;


public class TwopcSample {
	private BitSet Destination;
	private AtomicInteger OKcount;
	private AtomicInteger Receivedcount;
	private Set<Integer> VotedIds; 
	private AtomicBoolean finish;
	private Set<RequestId> collectedSet = new ConcurrentSkipListSet<RequestId>();



	public TwopcSample() {
		Destination=new BitSet();
		this.finish=new AtomicBoolean(false);
		this.OKcount=new AtomicInteger(0);
		this.Receivedcount=new AtomicInteger(0);
		VotedIds=new HashSet<Integer>();	
	}



	public void addToCollectedSet(Set<RequestId> cSet, RequestId reqId) {
		synchronized(this.collectedSet) {
			for(RequestId r: cSet) {
				if(this.collectedSet.add(r)) {
				}
			}
		}
	}



	public Set<RequestId> getCollectedSet() {
		synchronized(this.collectedSet) {
			return this.collectedSet;
		}
	}


	public synchronized boolean setFinish() {

		return this.finish.compareAndSet(false, true);	
	}



	public boolean getFinishe() {
		return this.finish.get();
	}

	public void addToVotedIds(int id){
		VotedIds.add((Integer)id);
	}

	public Set<Integer> getVotedIds(){
		return this.VotedIds;
	}


	public void SetDestination(BitSet Destination){
		this.Destination=Destination;
	}

	public BitSet getDestinations(){
		return Destination;
	}



	public int getDestNum(){
		return Destination.cardinality();
	}

	public int getOKcount(){
		return OKcount.get();
	}

	public void incrementOKcount(){
		OKcount.addAndGet(1);
	}



	public int getReceivedcount(){
		return Receivedcount.get();
	}

	public void incrementReceivedcount(){
		Receivedcount.addAndGet(1);
	}

}