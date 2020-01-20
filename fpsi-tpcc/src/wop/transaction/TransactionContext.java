package wop.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import wcc.common.RequestId;





public class TransactionContext {
	WriteSet writeset = new WriteSet();
	private ReadSet readset = new ReadSet();
	List<String>  RemoteReadsIds;
	private int SeqNo;
	private AtomicIntegerArray StartVTS ;
	AtomicBoolean WaitingRead;
	AtomicBoolean ready;
	AtomicBoolean retry;
	private int numNodes;
	private AtomicInteger retryTimes;
	private int[] TransactionhasRead;
	AtomicBoolean readyToRemove;
	public int isUpdate;
	private int wsizeByte;
	private int rsizeByte;


	public TransactionContext(int numNodes, int retrytimes, int isUpdate){
		StartVTS= new AtomicIntegerArray(numNodes); 
		this.RemoteReadsIds =  new ArrayList<String>();
		this.WaitingRead=new AtomicBoolean(false);
		this.ready=new AtomicBoolean(false);
		this.retry=new AtomicBoolean(false);
		this.readyToRemove=new AtomicBoolean(false);
		this.numNodes=numNodes;
		this.retryTimes=new AtomicInteger(retrytimes);
		TransactionhasRead=new int[numNodes];
		for(int i=0; i<numNodes; i++) {
			TransactionhasRead[i]=0;
		}		
		this.isUpdate=isUpdate;
	}

	public int getRetryTimes() {
		return this.retryTimes.get();
	}

	public int incrementRetryTimes() {
		return this.retryTimes.incrementAndGet();
	}


	public void setReadyToRemove() {
		 this.readyToRemove.set(true);
	}
	
	public boolean getReadyToRemove() {
		return this.readyToRemove.get();
	}
	
	public void AddIds(String Id){
		RemoteReadsIds.add(Id);
	}

	public List<String> RemoteReadsIds(){
		return RemoteReadsIds;
	}


	public void SetSeqNo(int SeqNo){
		this.SeqNo=SeqNo;
	}

	public int getSeqNo(){
		return SeqNo;
	}

	public AtomicIntegerArray getStartVC(){
		return this.StartVTS;
	}


	public int[] getStartVCArray() {
		int[] vc=new int [this.numNodes];
		for(int i=0;i<this.numNodes;i++) {
			vc[i]=StartVTS.get(i);
		}
		return vc;
	}


	public void setStarVTS(int[] replicastatus){
		StartVTS= new AtomicIntegerArray(replicastatus);
	}

	public void addObjectToReadSet(String objId, ReadSetEntry entry, String place) {
     	readset.addToReadSet(objId, entry);
		}
	

	public void addObjectToWriteSet(String objId, AbstractObject object) {
		writeset.addToWriteSet(objId,object);
	}

	public Map<String, AbstractObject> getWriteSet() {
		return writeset.getWriteSet();
	}


	public Map<String, ReadSetEntry> getReadSet() {
			return readset.getReadSet();
		}


	public void setWaitingRead(RequestId reqId) {
		if(this.WaitingRead.compareAndSet(false, true))
		{
		}
		else {
			System.out.println("WaitingRead  must be false BUT it is "+WaitingRead.get()+" for requestId "+reqId);
			System.exit(-1);
		}
	}

	public void resetWaitingREad(RequestId reqId) {
		if(this.WaitingRead.compareAndSet(true, false)) {
		}
		else {
			System.out.println("WaitingRead  must be true BUT it is "+false+" for requestId "+reqId);
			System.exit(-1);
		}
	}

	public void setReady (String place, RequestId reqId) {
		if(this.ready.get()) {
			System.out.println("There is a pRoblem with ready in place "+place+" for reqId "+reqId);
			System.exit(-1);
		}
		else {
			this.ready.compareAndSet(false, true);
		}
	}

	public boolean getReady() {
		return this.ready.get();
	}

	public void setRetry() {
		this.retry.compareAndSet(false, true);
	}

	public boolean getRetry() {
		return this.retry.get();
	}



	public boolean getWaitingRead() {
		return this.WaitingRead.get();

	}



	public int[] getTransactionhasRead(){
		return  this.TransactionhasRead;
	}


	public void setTransactionhasRead(RequestId requestId,int localId, int siteId, int isUpdate){		
		if(isUpdate==0) {
		TransactionhasRead[localId]=1;
		}
		else {
			TransactionhasRead[localId]=1;
			TransactionhasRead[siteId]=1;
			
		}
	}

	public void setTransactionVC(RequestId requestId ,int[] vc,  int place) {
		if(place==3) {
			for(int i=0; i<numNodes;i++) {
				if(StartVTS.get(i)<vc[i]) {
					StartVTS.set(i, vc[i]);
				}
			}
		}
		else {
			for(int i=0; i<numNodes;i++) {
				if(StartVTS.get(i)<vc[i] && TransactionhasRead[i]!=1) {
					StartVTS.set(i, vc[i]);
				}
			}
		}
	}

	
	
	public void setWriteSetByteSize(int wsizeByte) {
		this.wsizeByte=wsizeByte;
	}

	public void setReadSEtByteSIze(int rsizeByte) {
		this.rsizeByte=rsizeByte;

	}



	public int getWriteSetByteSize() {
		return this.wsizeByte;
	}

	public int getReadSetByteSIze() {
		return this.rsizeByte;

	}
}