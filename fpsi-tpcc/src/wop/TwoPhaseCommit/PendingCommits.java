package wop.TwoPhaseCommit;

import java.util.concurrent.PriorityBlockingQueue;


public class PendingCommits {
	PriorityBlockingQueue<PendingCommit> pendQ = new PriorityBlockingQueue<PendingCommit>();



	public void AddToPendQ(PendingCommit entry) {
		synchronized(this.pendQ) {
			pendQ.offer(entry);
		}
	}

	public void removeFromPendQ(PendingCommit entry) {
		synchronized(this.pendQ) {
			pendQ.remove(entry);
		}
	}

	public PriorityBlockingQueue<PendingCommit> getPendQ(){
		synchronized(this.pendQ) {
			return this.pendQ;
		}
	}

}
