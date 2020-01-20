package wcc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import wcc.common.RequestId;
import wop.service.STMService;
import wop.transaction.AbstractObject;
import wop.transaction.ReadSetEntry;
import wop.transaction.TransactionContext;
import wop.TwoPhaseCommit.PendingCommit;
import wop.TwoPhaseCommit.PendingCommits;
import wop.messages.Read;

public class Wcc {
	Store store;
	private STMService service;
	int localId;
	int numNodes;
	private ConcurrentHashMap<RequestId, TransactionContext> requestIdContextMap;
	private ConcurrentHashMap<Integer, PendingCommits> pendingCommits;
	private NodeStatus nodestatus;
	AtomicInteger SeqNo;
	public final String TX_READ_MODE = "r";
	public final String TX_READ_WRITE_MODE = "rw";
	public final String Abort = "Abort Exception";
	public Range[] ranges;


	public Wcc(Store store, int numNodes, int localId) {
		this.store = store;
		this.localId = localId;
		this.numNodes = numNodes;
		nodestatus = new NodeStatus(numNodes);
		requestIdContextMap = new ConcurrentHashMap<RequestId, TransactionContext>();
		pendingCommits = new ConcurrentHashMap<Integer, PendingCommits>();
		for (int i = 0; i < numNodes; i++) {
			pendingCommits.put((Integer) i, new PendingCommits());
		}
		this.SeqNo = new AtomicInteger(0);
		if(localId==0) {
			this.ranges=new Range[2];
			int mini=store.getAccessibleObject() * this.localId;
			int maxi=(store.getAccessibleObject() * (this.localId + 1)) - 1;
			ranges[0]=new Range (mini,maxi);
			mini=store.getAccessibleObject() * (this.numNodes-1);
			maxi=(store.getAccessibleObject() * (this.numNodes-1 + 1)) - 1;
			ranges[1]=new Range(mini,maxi);
		}
		else {
			this.ranges=new Range[1];
			int mini =store.getAccessibleObject() * (this.localId-1);
			int maxi=(store.getAccessibleObject() * (this.localId + 1)) - 1;

			ranges[0]=new Range (mini, maxi );
		}
		/*	for(int i=0;i<ranges.length; i++) {
			System.out.println("For node "+this.localId+"ranges min is/are "+ranges[i].getMin());
			System.out.println("For node "+this.localId+"ranges max is/are "+ranges[i].getMax());
		}
		 */
	}


	public Range[] getRanges() {
		return this.ranges;
	}


	public void init(STMService service) {
		this.service = service;
	}

	public STMService getSTMService() {
		return this.service;
	}

	public NodeStatus getNodeStatus() {
		return this.nodestatus;
	}

	public boolean IfReplicate(String objId) {
		return store.Ifreplicate(objId);
	}

	public int getLocalId() {
		return this.localId;
	}

	public Store getStore() {
		return this.store;
	}

	public int getSeqNo() {
		return SeqNo.get();
	}

	public synchronized int incrementSeqNo(RequestId reqId) {
		SeqNo.addAndGet(1);
		return this.SeqNo.get();
	}

	public PendingCommits getPendingCommits(int nodeId) {
		return this.pendingCommits.get((Integer) nodeId);
	}

	public void createTransactionContext(RequestId requestId, int retryTimes, int isUpdate) {
		if (!requestIdContextMap.containsKey(requestId)) {
			TransactionContext ctx = new TransactionContext(numNodes,retryTimes, isUpdate);
			int[] vc = new int[numNodes];
			vc = this.nodestatus.getNodeStatus();
			ctx.setStarVTS(vc);
			requestIdContextMap.put(requestId, ctx);
		}
	}

	public void removeTransactionContext(RequestId requestId) {
		if (requestIdContextMap.containsKey(requestId)) {
			requestIdContextMap.remove(requestId);
		} else {
			System.out.println("RequestIdContextMap does not contain requestId " + requestId);
			System.exit(-1);
		}
	}

	public TransactionContext getlocalTransactionContext(RequestId requestId) {
		return requestIdContextMap.get(requestId);
	}

	public AbstractObject Write(String objId, AbstractObject obj, RequestId requestId, int retryTimes) {
		createTransactionContext(requestId,retryTimes,1);
		requestIdContextMap.get(requestId).addObjectToWriteSet(objId, obj);
		return obj;
	}

	public void notifyforRead(RequestId reqId, String Id, AbstractObject object, int[] maxVC, 
			int sender, int retryTimes, int siteId, int objectSeqNo) {
		if(retryTimes==-1) {
			if (reqId != null) {
				TransactionContext ctx = requestIdContextMap.get(reqId);
				ctx.setTransactionVC(reqId,maxVC,1);
				if (ctx.getTransactionhasRead()[sender] == 0) {
					ctx.setTransactionhasRead(reqId,sender, siteId, ctx.isUpdate);
				}
				ctx.addObjectToReadSet(Id, new ReadSetEntry(object, siteId, objectSeqNo), "line 161");
				ctx.resetWaitingREad(reqId);
			} else {
				System.out.println("WitingRequests does not contain reqId");
				System.exit(-1);
			}
		}
		if(retryTimes!=-1) {
			if(retryTimes==requestIdContextMap.get(reqId).getRetryTimes()) {
				if (reqId != null) {
					TransactionContext ctx = requestIdContextMap.get(reqId);
					ctx.addObjectToReadSet(Id, new ReadSetEntry(object, siteId, objectSeqNo), "line 172");
					ctx.resetWaitingREad(reqId);
				} else {
					System.out.println("WitingRequests does not contain reqId!!!!!!");
					System.exit(-1);
				}				
			}
		}
	}

	public AbstractObject Read(String objId, String txMode, RequestId requestId, int retryTimes)
			throws IOException, InterruptedException {
		AbstractObject object = null;
		int isUpdate = 0;//read-only
		if (txMode.equals("rw")) {
			isUpdate = 1;
		}
		createTransactionContext(requestId,retryTimes,isUpdate);
		TransactionContext ctx = requestIdContextMap.get(requestId);
		if (ctx.getWriteSet().containsKey(objId)) {
			object = ctx.getWriteSet().get(objId);
		}
		if (store.Ifreplicate(objId)) {
			int [] hRead= new int[this.numNodes];
			for (int i=0 ;i<this.numNodes; i++) {
				hRead[i]=ctx.getTransactionhasRead()[i];
			}
			
			ReadResponse resp=store.getLatestObject1(requestId, objId, ctx.getStartVCArray(),hRead, this.numNodes, isUpdate, this.localId);
			object=resp.getObject();
				ctx.setTransactionVC(requestId, resp.suggestedVC, 7);
			if (ctx.getTransactionhasRead()[localId] == 0) {
				ctx.setTransactionhasRead(requestId,localId, resp.siteId,isUpdate);
			}
			requestIdContextMap.get(requestId).addObjectToReadSet(objId, new ReadSetEntry(object, resp.siteId, resp.objSeqNo), "line 207");
		}
		if (!store.Ifreplicate(objId)) {
			Read read = new Read(requestId, objId.length(), objId, numNodes, serializeVC(ctx.getStartVC()),serializeHasRead(ctx.getTransactionhasRead()),isUpdate,retryTimes);
			requestIdContextMap.get(requestId).setWaitingRead(requestId);
			service.getNode().getProcessinRead().SendRead(read, getDestination(objId));
			while (requestIdContextMap.get(requestId).getWaitingRead()) {
			}
			if (requestIdContextMap.get(requestId).getReadSet().containsKey(objId)) {
				object = requestIdContextMap.get(requestId).getReadSet().get(objId).getObject();
			} else {
				System.out.println("There is no readSet Element");
				System.exit(-1);
			}
		}
		return object;
	}

	public boolean IsObjectUnlocked(String Id, int updator, RequestId requestId) {
		return store.getObjectHistory(Id).LockHistory(Id,updator, requestId);
	}

	public boolean IsObjectVisible(RequestId reqId, String Id, int[] StartVTS) {
		Version lastversion = store.getLastVersion(Id);
		return (StartVTS[lastversion.getlocalId()] >= lastversion.getSeqNo());
	}

	public void UnLockedbyLocker(String Id, int unlockerSite, RequestId requestId, String place) {
		store.getObjectHistory(Id).UnLockedbyLocker(unlockerSite, requestId, Id, place);
	}

	public byte[] serializeVC(AtomicIntegerArray TransactionVC) {
		ByteBuffer bb = ByteBuffer.allocate((TransactionVC.length()) * 4);
		for (int i = 0; i < TransactionVC.length(); i++) {
			bb.putInt(TransactionVC.get(i));
		}
		return bb.array();
	}


	public byte[] serializeVC(int[] TransactionVC) {
		ByteBuffer bb = ByteBuffer.allocate((TransactionVC.length) * 4);
		for (int i = 0; i < TransactionVC.length; i++) {
			bb.putInt(TransactionVC[i]);
		}
		return bb.array();
	}

	public int[] deserializeVC(byte[] vc) {
		ByteBuffer bb = ByteBuffer.wrap(vc);
		int[] VC = new int[numNodes];
		for (int i = 0; i < this.numNodes; i++) {
			VC[i] = bb.getInt();
		}
		return VC;
	}

	public boolean IsUpdateOK(RequestId requestId, int[] StartVTS, Map<String, AbstractObject> writeset, int updator,
			String place) {
		boolean update = true;
		HashSet<String> LockedIds = new HashSet<String>();
		Iterator<String> iterator = writeset.keySet().iterator();
		while (iterator.hasNext() && update != false) {
			String Id = (String) iterator.next();
			if (store.Ifreplicate(Id)) {
				synchronized(store.getStore().get(Id)){
					if (!IsObjectUnlocked(Id, updator, requestId)) {
						update = false;
					} else {
						LockedIds.add(Id);
					}
					if (update != false) {

						if (!IsObjectVisible(requestId, Id, StartVTS)) {
							update = false;
						}
					}
					if (update == false) {
						for (String s : LockedIds) {
							UnLockedbyLocker(s, updator, requestId, " IS UPDATE OK");
						}
					}
				}
			}

		}
		return update;
	}



	public Set<RequestId> collectAntiDep(RequestId requestId, int[] StartVTS, Map<String, AbstractObject> writeset, int updator) {
		Set<RequestId> collection=new HashSet<RequestId>();
		for(String Id:writeset.keySet()) {
			if(store.Ifreplicate(Id)) {
					boolean done=true;
					try {

						while(done) {
							synchronized( store.getLastVersion(Id)) {
							Version lastversion = store.getLastVersion(Id);
							Set<RequestId> mainReqs=null;
							if(lastversion.getSeqNo()==0) {
								int defaultLocalId=this.numNodes-1;
								mainReqs=store.getObjectHistory(Id).getSitehistory(defaultLocalId).getMainRWInfo(Id, lastversion.getSeqNo());
							}
							else {	
								mainReqs=store.getObjectHistory(Id).getSitehistory(lastversion.getlocalId()).getMainRWInfo(Id, lastversion.getSeqNo());
							}
							synchronized(mainReqs) {
								mainReqs=store.getObjectHistory(Id).getSitehistory(lastversion.getlocalId()).getMainRWInfo(Id, lastversion.getSeqNo());
								for(RequestId req: mainReqs) {
									if(req.getClientId()==requestId.getClientId() && req.getSeqNumber()<requestId.getSeqNumber()) {
										if(mainReqs.remove(req)) {

										}
									}
									else {
										if(collection.add(req)){
										}
									}	
								}
								
							}
							done=false;
						}
						}
					}
					catch(ConcurrentModificationException e){

					}
			}
		}
		return collection;

	}




	public void ReleaseLock(TransactionContext ctx, int updator, RequestId requestId, String place) {
		for (String Id : ctx.getWriteSet().keySet()) {
			if (store.Ifreplicate(Id)) {
				store.getObjectHistory(Id).UnLockedbyLocker(updator, requestId, Id, place);
			}
		}
	}

	public  synchronized void FinishCommit(RequestId reqId, Map<String, AbstractObject> WriteSet, int sn, int updator,
			String place, int[] commitVTS, Set<RequestId> collectedSet) throws InterruptedException  {
		synchronized (getPendingCommits((Integer) updator)) {
			PendingCommit QHead = getPendingCommits().get((Integer) updator).getPendQ().peek();
			int committing =sn - 1;
			if(getNodeStatus().getNodeStatus()[updator]==committing-1) {
				if (QHead != null && QHead.getSeqNo() == committing) {
					//System.out.println("ReqId "+reqId+" finds reqId "+QHead.getRequestId()+" and is helping to commit it in LIne 262");
					if(QHead.getWriteSet()!=null) {
						for (String Id : QHead.getWriteSet().keySet()) {
							if (getStore().Ifreplicate(Id)) {
								getStore().updateStore(QHead.getRequestId(), QHead.getUpdator(), Id, QHead.getSeqNo(), 
										QHead.getWriteSet().get(Id), " line 461");
								for(RequestId r: collectedSet) {							
									int siteId= getStore().getLastVersion(Id).getlocalId();
									int sNumber= getStore().getLastVersion(Id).getSeqNo();
										if(getStore().getStore().get(Id).getSitehistory(siteId).addToMainRWINfor(Id, sNumber, r)){
										}
								}
							}
						}
					}
					int[] commitVC= commitVTS;
					getNodeStatus().UpdateNodeStatus(QHead.getRequestId(), QHead.getSeqNo(), QHead.getUpdator(),"Line 392");
					if (QHead.getWriteSet() != null) {
						for (String Id : QHead.getWriteSet().keySet()) {
							if (getStore().Ifreplicate(Id)) {
								getStore().getLastVersion(Id).getObject().setVC(commitVC);
								ReleaseLock(Id, QHead.getUpdator(), QHead.getRequestId(),
										"Line 323 Finish Commit");
							}

						}
					}
					PendingCommit l=getPendingCommits().get((Integer) updator).getPendQ().poll();
					System.out.println("ReqId "+reqId+" removes "+l.getSeqNo());
					committing=committing-1;
					QHead = getPendingCommits().get((Integer) updator).getPendQ().peek();
				}
			}
			/***End of commiting lesser seqNo**/
			if (getNodeStatus().getNodeStatus()[updator] == (sn - 1)) {// cjeck committedVTS
				for (String Id : WriteSet.keySet()) {
					if (getStore().Ifreplicate(Id)) {
						getStore().updateStore(reqId, updator, Id, sn, WriteSet.get(Id)/*, startVTS*/," line 531");
						for(RequestId r: collectedSet) {
							int siteId= getStore().getLastVersion(Id).getlocalId();
							int sNumber= getStore().getLastVersion(Id).getSeqNo();
								if(getStore().getStore().get(Id).getSitehistory(siteId).addToMainRWINfor(Id, sNumber, r)){
								}
						}
					}
				}
				int[] commitVC= commitVTS;
				getNodeStatus().UpdateNodeStatus(reqId, sn, updator, "Line 323");
				for (String Id : WriteSet.keySet()) {
					if (store.Ifreplicate(Id)) {
						getStore().getLastVersion(Id).getObject().setVC(commitVC);
						ReleaseLock(Id, updator, reqId, "Line 314 Finish Commit");
					}
				}
				QHead = this.pendingCommits.get((Integer) updator).getPendQ().peek();
				sn = sn + 1;
				while (QHead != null && QHead.getSeqNo() == sn) {
					if(QHead.getWriteSet()!=null) {
						for (String Id : QHead.getWriteSet().keySet()) {
							if (getStore().Ifreplicate(Id)) {
								getStore().updateStore(QHead.getRequestId(), QHead.getUpdator(), Id, QHead.getSeqNo(), 
										QHead.getWriteSet().get(Id)/*,startVTS*/," line 593");
							}
						}
					}
					int[] commitVC1= QHead.getCommitVC();
					getNodeStatus().UpdateNodeStatus(QHead.getRequestId(), QHead.getSeqNo(), QHead.getUpdator(), "Line 350");
					if(QHead.getWriteSet()!=null) {
						for (String Id : QHead.getWriteSet().keySet()) {
							if (getStore().Ifreplicate(Id)) {
								for(RequestId r: QHead.getCollectedSet()) {
									int siteId= getStore().getLastVersion(Id).getlocalId();
									int sNumber= getStore().getLastVersion(Id).getSeqNo();
										if(getStore().getStore().get(Id).getSitehistory(siteId).addToMainRWINfor(Id, sNumber, r)) {	
										}
								}
							}
							if (store.Ifreplicate(Id)) {
								getStore().getLastVersion(Id).getObject().setVC(commitVC1);
								ReleaseLock(Id, QHead.getUpdator(), QHead.getRequestId(), "Libe 334 Finish Commit");
							}
						}
					}
					PendingCommit l=this.pendingCommits.get((Integer) updator).getPendQ().poll();
					sn = sn + 1;
					QHead = this.pendingCommits.get((Integer) updator).getPendQ().peek();
				}
			}
			else {
				int[] commitVC= commitVTS;
				getPendingCommits((Integer)updator).AddToPendQ(new PendingCommit(reqId,WriteSet, sn, updator, commitVC, collectedSet));
			}
		}
	}


	public ConcurrentHashMap<Integer, PendingCommits> getPendingCommits() {
		return this.pendingCommits;
	}

	public void ReleaseLock(String Id, int updator, RequestId requestId, String place) {
		store.getObjectHistory(Id).UnLockedbyLocker(updator, requestId, Id, place);
	}

	public BitSet getDestination(String Id) {
		return store.getContainers(Id);
	}

	public int getNumNodes() {
		return this.numNodes;
	}


	public byte[] serializeHasRead(int[] hasReas) {
		ByteBuffer bb = ByteBuffer.allocate(hasReas.length * 4);
		for (int i = 0; i < hasReas.length; i++) {
			bb.putInt(hasReas[i]);
		}
		return bb.array();
	}


	public int[] deserializehasRead(byte[] hasRead) {
		if (hasRead == null) {
			System.out.println("hasRead is null");
			System.exit(-1);
		}
		ByteBuffer bb = ByteBuffer.wrap(hasRead);
		int[] HasRead = new int[numNodes];
		for (int i = 0; i < this.numNodes; i++) {
			int value = bb.getInt();
			HasRead[i]=value;
		}
		return HasRead;
	}


}
