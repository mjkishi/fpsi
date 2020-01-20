package wop.TwoPhaseCommit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import wcc.Wcc;
import wcc.common.RequestId;
import wop.messages.Abort;
import wop.messages.Commit;
import wop.messages.Message;
import wop.messages.MessageType;
import wop.messages.Prepare;
import wop.messages.PrepareAns;
import wop.messages.Propagate;
import wop.network.MessageHandler;
import wop.network.Network;
import wop.network.TcpNetwork;
import wop.transaction.AbstractObject;
import wop.transaction.TransactionContext;

public class Processintwopc {
	private final TcpNetwork network;
	private Wcc wccInstance;
	int localId;
	private ConcurrentHashMap<RequestId, TwopcSample> TwopcMap;
	public int numNodes;
	private static ExecutorService RequestDispatcher;
	PriorityBlockingQueue<PendingCommit> PropagateQ = new PriorityBlockingQueue<PendingCommit>();
	NodeStatusUpdator  nodeStatusHandler;
	ConcurrentLinkedQueue<PendingCommit> propagationList;

	public Processintwopc(Wcc wccInstance, int localId) throws IOException {
		this.wccInstance = wccInstance;
		this.network = new TcpNetwork(0);
		this.localId = localId;
		TwopcMap = new ConcurrentHashMap<RequestId, TwopcSample>();
		this.numNodes = wccInstance.getNumNodes();
		RequestDispatcher = Executors.newFixedThreadPool(20);
		propagationList=new ConcurrentLinkedQueue<PendingCommit> ();
	}

	public void startTwopcforProcess() {
		MessageHandler handler = new MessageHandlerImpl();
		Network.addMessageListener(MessageType.Prepare, handler);
		Network.addMessageListener(MessageType.PrepareAns, handler);
		Network.addMessageListener(MessageType.Commit, handler);
		Network.addMessageListener(MessageType.Abort, handler);
		Network.addMessageListener(MessageType.Propagate, handler);
		network.start();
		nodeStatusHandler=new NodeStatusUpdator(this);
		this.nodeStatusHandler.start();
	}

	public void removeTwopcSample(RequestId requestId) {
		if (TwopcMap.containsKey(requestId)) {
			TwopcMap.remove(requestId);
		}
	}

	public Wcc getWccInstance() {
		return this.wccInstance;
	}

	public void Prepare(RequestId requestId) throws IOException, InterruptedException {
		TransactionContext ctx = wccInstance.getlocalTransactionContext(requestId);
		BitSet finalDestination = new BitSet();
		TwopcSample twopcOK = new TwopcSample();
		byte[] vts = wccInstance.serializeVC(ctx.getStartVC());
		Iterator<String> iterator = ctx.getWriteSet().keySet().iterator();
		while (iterator.hasNext()) {
			String Id = (String) iterator.next();
			BitSet Destination = wccInstance.getDestination(Id);
			for (int i = 0; i < numNodes; i++) {
				if (Destination.get(i)) {
					finalDestination.set(i);
				}
			}

		}
		twopcOK.SetDestination(finalDestination);
		TwopcMap.put(requestId, twopcOK);
		int probablesn = (wccInstance.getNodeStatus().getNodeStatus()[localId]);
		byte[] writeSet=wccInstance.getSTMService().serializeWriteSet(ctx);
		Prepare msg = new Prepare(requestId, probablesn, ctx.getWriteSet().size(), ctx.getWriteSetByteSize(),
				writeSet, vts, this.numNodes);
		int recivers = 0;
		for (int i = finalDestination.nextSetBit(0); i >= 0; i = finalDestination.nextSetBit(i + 1)) {
			network.sendMessage(msg, i);
			recivers = recivers + 1;
			while (twopcOK.getReceivedcount() != recivers) {
			}
			if (ctx.getReady()) {
				break;
			}
		}

	}

	public  void onPrepare(Prepare prepare, int sender) throws IOException {
		boolean ok = true;
		int[] startVTS = wccInstance.deserializeVC(prepare.getStartVC());
		Map<String, AbstractObject> writeset = wccInstance.getSTMService().deserializeWriteSet(prepare.getwsize(),prepare.getwriteSet());
		ok = wccInstance.IsUpdateOK(prepare.getRequestId(), startVTS, writeset, sender, " Is UpdateOK on prepare");
		Set<RequestId> collection;
		if (!ok) {
			byte[] collectionByte=new byte[prepare.getRequestId().byteSize()];
			collectionByte=prepare.getRequestId().toByteArray();
			SendPrepareNOK(prepare, sender, collectionByte.length,collectionByte);
		} else {
			collection=wccInstance.collectAntiDep(prepare.getRequestId(), startVTS, writeset, sender);
			byte[] collectionByte=this.serializeCollectionSet(prepare.getRequestId(), collection);
			SendPrepareOK(prepare, sender, collectionByte.length,collectionByte );
		}
	}

	public byte[] serializeCollectionSet(RequestId reqId,Set<RequestId> collection) throws IOException {	
		ByteBuffer bb;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		for (RequestId requestId : collection) {
			if (requestId!=null) {
				bb=ByteBuffer.allocate(requestId.byteSize());
				bb.put(requestId.toByteArray());
				bb.flip();
				out.write(bb.array());
			}
			else {
				System.out.println("Error");
				System.exit(-1);
			}
		}
		return out.toByteArray();
	}

	public Set<RequestId> deserializeCollectionSet(RequestId reqId ,byte[] collectionByte , int sizeOfCollectionByte){
		ByteBuffer bb = ByteBuffer.wrap(collectionByte);
		Set<RequestId> collectionSet=new HashSet<RequestId>(sizeOfCollectionByte);
		for (int i = 0; i < sizeOfCollectionByte/reqId.byteSize(); i++) {
			RequestId requestId=new RequestId (bb.getLong(), bb.getInt());
			if( collectionSet.add(requestId)) {
			}
		}
		return collectionSet;
	}


	public void SendPrepareOK(Prepare msg, int sender, int collectionByteSize, byte[]  collectionByte) { 
		PrepareAns m = new PrepareAns(msg, 0,collectionByteSize, collectionByte);
		network.sendMessage(m, sender);
	}

	public void SendPrepareNOK(Prepare msg, int sender, int collectionByteSize, byte[]  collectionByte) { // onPrepare ----CORRECT
		PrepareAns m = new PrepareAns(msg, 1,collectionByteSize, collectionByte);
		network.sendMessage(m, sender);
	}

	public  void onPrepareAns(PrepareAns msg, int sender) throws IOException, InterruptedException {
		if (msg.getSuccess() == 0) {
			TwopcSample twopcOK = TwopcMap.get(msg.getRequestId());
			twopcOK.addToCollectedSet(deserializeCollectionSet(msg.getRequestId(), msg.getCollectedRW(), msg.getSizeOfCollectedRW()), msg.getRequestId());
			twopcOK.addToVotedIds(sender);
			twopcOK.incrementOKcount();
			twopcOK.incrementReceivedcount();
			TwopcMap.put(msg.getRequestId(), twopcOK);
			if (twopcOK.getOKcount() == twopcOK.getDestNum()) {
				twopcOK.incrementOKcount();
				wccInstance.getSTMService().IncreaseCommitCount();
				BitSet PropagationDest=SendCommit((PrepareAns) msg);
				wccInstance.getSTMService().addToCollectedNum(twopcOK.getCollectedSet().size());	
				wccInstance.getlocalTransactionContext(msg.getRequestId()).setReady("SEND COMMIT Line 340", msg.getRequestId());
				PendingCommit pc=new PendingCommit(msg.getRequestId(),wccInstance.getlocalTransactionContext(msg.getRequestId()).getSeqNo(), wccInstance.getlocalTransactionContext(msg.getRequestId()).getStartVCArray(),PropagationDest);	
				propagationList.add(pc);	
				while(!wccInstance.getlocalTransactionContext(msg.getRequestId()).getReadyToRemove()) {}
				wccInstance.removeTransactionContext(msg.getRequestId());
				wccInstance.getSTMService().getNode().getProcessinRead().removeReadReq(msg.getRequestId());
				wccInstance.getSTMService().getNode().getProcessintwopc().removeTwopcSample(msg.getRequestId());
			}
		} else {
			onPrepareNOK(msg, sender);
		}
	}


	public void onPrepareNOK(PrepareAns msg, int sender) throws IOException {
		RequestId requestId = msg.getRequestId();
		TransactionContext ctx = wccInstance.getlocalTransactionContext(requestId);
		if (TwopcMap.get(msg.getRequestId()).getVotedIds().size() != 0) {
			byte[] wset = wccInstance.getSTMService().serializeWriteSet(ctx);
			int wsize = ctx.getWriteSet().size();
			Abort a = new Abort(msg, wsize, wset);
			boolean nextIf=true;
			for (Integer id : TwopcMap.get(msg.getRequestId()).getVotedIds()) {
				if(TwopcMap.get(msg.getRequestId()) !=null && TwopcMap.get(msg.getRequestId()).getVotedIds().contains(this.localId)) {
					nextIf=false;
				}
				network.sendMessage(a, id);
			}

			if (nextIf && TwopcMap.get(msg.getRequestId()) != null && !TwopcMap.get(msg.getRequestId()).getVotedIds().contains(this.localId)) {
				ctx.setRetry();
				ctx.setReady("onPrepareNOK Line 195", msg.getRequestId());
				TwopcMap.get(msg.getRequestId()).incrementReceivedcount();
			}

		} else {
			ctx.setRetry();
			ctx.setReady("onPrepareNOK Line 208", msg.getRequestId());
			TwopcMap.get(msg.getRequestId()).incrementReceivedcount();
		}
	}


	public void onCommit(Commit msg, int sender) throws InterruptedException { // onCommit ---CORRECT
		int wsize = msg.getwsize();
		byte[] wset = msg.getwriteSet();
		Map<String, AbstractObject> WriteSet = wccInstance.getSTMService().deserializeWriteSet(wsize, wset);
		int sn = msg.getSeqNo();
		int[] commitVTS=wccInstance.deserializeVC(msg.getCommitVTS());
		Set<RequestId> collectedSet=this.deserializeCollectionSet(msg.getRequestId(), msg.getCollectedRW(), msg.getSizeOfCollectedRW());
		wccInstance.FinishCommit(msg.getRequestId(), WriteSet, sn, sender, "Finish Commit onCommit",commitVTS,collectedSet);

	}

	public void onAbort(Abort msg, int sender) throws IOException {
		int wsize = msg.getwsize();
		byte[] wset = msg.getwriteSet();
		Map<String, AbstractObject> WriteSet = wccInstance.getSTMService().deserializeWriteSet(wsize, wset);
		for (Map.Entry<String, AbstractObject> entry : WriteSet.entrySet()) {
			String Id = entry.getKey();
			if (wccInstance.getStore().Ifreplicate(Id)) {
				wccInstance.ReleaseLock(Id, sender, msg.getRequestId(), "Line 247 on Abort");
			}
		}

		if (sender == this.localId) {
			TransactionContext ctx = wccInstance.getlocalTransactionContext(msg.getRequestId());
			ctx.setRetry();
			ctx.setReady("In ON ABORT Line 234", msg.getRequestId());
			TwopcMap.get(msg.getRequestId()).incrementReceivedcount();
		}
	}


	public synchronized BitSet SendCommit(PrepareAns msg) throws IOException, InterruptedException {
		int[] commitVC=new int[this.numNodes];
		RequestId requestId = msg.getRequestId();
		TransactionContext ctx = wccInstance.getlocalTransactionContext(requestId);
		BitSet PropagationDest = new BitSet();
		Set<RequestId> collectedSet= TwopcMap.get(msg.getRequestId()).getCollectedSet();
		int sn = wccInstance.incrementSeqNo(msg.getRequestId());
		if(ctx!=null) {
			ctx.SetSeqNo(sn);
			for (Map.Entry<String, AbstractObject> entry : ctx.getWriteSet().entrySet()) {
				AbstractObject obj = entry.getValue();
				String Id = entry.getKey();
				if (wccInstance.getStore().Ifreplicate(Id)) {
					wccInstance.getStore().updateStore(requestId, this.localId, Id, sn, obj/*,ctx.getStartVCArray()*/," line 361");
					for(RequestId r: collectedSet) {
						int siteId= wccInstance.getStore().getLastVersion(Id).getlocalId();
						int sNumber= wccInstance.getStore().getLastVersion(Id).getSeqNo();
						if(wccInstance.getStore().getStore().get(Id).getSitehistory(siteId).addToMainRWINfor(Id, sNumber, r)){
						}
					}
				}
			}
			if (wccInstance.getNodeStatus().getNodeStatus()[this.localId] == sn - 1) {
				commitVC=wccInstance.getNodeStatus().UpdateNodeStatus(msg.getRequestId(), sn, this.localId,"Line 346");
				for (String Id : ctx.getWriteSet().keySet()) {
					if (wccInstance.getStore().Ifreplicate(Id)) {
						wccInstance.getStore().getLastVersion(Id).getObject().setVC(commitVC);
						wccInstance.ReleaseLock(Id, this.localId, msg.getRequestId(), "Line 304 Send Commit");
					}
				}
				sn = sn + 1;
				synchronized (wccInstance.getPendingCommits((Integer) this.localId)) {
					PendingCommit QHead = wccInstance.getPendingCommits(this.localId).getPendQ().peek();
					while (QHead != null && QHead.getSeqNo() == sn) {
						if(QHead.getWriteSet()!=null) {
							for (String Id : QHead.getWriteSet().keySet()) {
								if (wccInstance.getStore().Ifreplicate(Id)) {
									wccInstance.getStore().updateStore(QHead.getRequestId(), QHead.getUpdator(), Id, QHead.getSeqNo(), 
											QHead.getWriteSet().get(Id), " line 423");
									for(RequestId r: QHead.getCollectedSet()) {
										int siteId= wccInstance.getStore().getLastVersion(Id).getlocalId();
										int sNumber= wccInstance.getStore().getLastVersion(Id).getSeqNo();
										if(wccInstance.getStore().getStore().get(Id).getSitehistory(siteId).addToMainRWINfor(Id, sNumber, r)){
										}
									}
								}
							}
						}
						wccInstance.getNodeStatus().UpdateNodeStatus(QHead.getRequestId(), QHead.getSeqNo(), QHead.getUpdator(), "LINE 371");
						for (String Id : QHead.getWriteSet().keySet()) {
							if (wccInstance.getStore().Ifreplicate(Id)) {
								wccInstance.ReleaseLock(Id, QHead.getUpdator(), QHead.getRequestId(),"Line 314 Send Commit");
							}

						}
						sn = sn + 1;
						QHead = wccInstance.getPendingCommits((Integer) localId).getPendQ().peek();
					}
				}
			} else {
				synchronized (wccInstance.getPendingCommits((Integer) this.localId)) {
					int[] commitVC1= wccInstance.getNodeStatus().getNodeStatus();
					commitVC1[this.localId]=sn;
					wccInstance.getPendingCommits((Integer) this.localId).AddToPendQ(new PendingCommit(msg.getRequestId(), ctx.getWriteSet(), sn, this.localId, commitVC1, collectedSet));
				}
			}
			byte[] writeSet = wccInstance.getSTMService().serializeWriteSet(ctx);
			byte[] commitVTSByte= wccInstance.serializeVC(commitVC);
			byte[] collectedSetByte=this.serializeCollectionSet(msg.getRequestId(), TwopcMap.get(msg.getRequestId()).getCollectedSet());
			Commit c = new Commit(msg, ctx.getWriteSet().size(),writeSet.length ,writeSet, this.numNodes, commitVTSByte, ctx.getSeqNo(), collectedSetByte.length,collectedSetByte);
			for (int k = 0; k < this.numNodes; k++) {
				PropagationDest.set(k);
			}
			PropagationDest.andNot(TwopcMap.get(msg.getRequestId()).getDestinations());
			PropagationDest.clear(this.localId);
			TwopcMap.get(msg.getRequestId()).getDestinations().clear(this.localId);
			if (TwopcMap.get(msg.getRequestId()).getDestinations().cardinality() != 0) {
				network.sendMessage(c, TwopcMap.get(msg.getRequestId()).getDestinations());
			}	
		}
		return PropagationDest;
	}





	public  void onPropagate(Propagate msg, int sender) throws InterruptedException {
		synchronized (wccInstance.getPendingCommits((Integer) sender)) {
			/***Commit lesser seqNo which are trapped in the Queue**/
			PendingCommit QHead = wccInstance.getPendingCommits().get((Integer) sender).getPendQ().peek();
			int committing = msg.getSeqNo() - 1;
			if(wccInstance.getNodeStatus().getNodeStatus()[sender]==committing-1) {
				if (QHead != null && QHead.getSeqNo() == committing) {
					if(QHead.getWriteSet()!=null) {
						for (String Id : QHead.getWriteSet().keySet()) {
							if (wccInstance.getStore().Ifreplicate(Id)) {
								wccInstance.getStore().updateStore(QHead.getRequestId(), QHead.getUpdator(), Id, QHead.getSeqNo(), QHead.getWriteSet().get(Id), "line 534 ");
							}
						}
					}
					wccInstance.getNodeStatus().UpdateNodeStatus(QHead.getRequestId(), QHead.getSeqNo(), QHead.getUpdator()," line 449");

					if (QHead.getWriteSet() != null) {
						for (String Id : QHead.getWriteSet().keySet()) {
							if (wccInstance.getStore().Ifreplicate(Id)) {
								wccInstance.ReleaseLock(Id, QHead.getUpdator(), QHead.getRequestId(),
										"Libe 323 Finish Commit");
							}

						}
					}
				}

			}			
			if (wccInstance.getNodeStatus().getNodeStatus()[sender] == (msg.getSeqNo() - 1)) {
				wccInstance.deserializeVC(msg.getStartVC());	
				wccInstance.getNodeStatus().UpdateNodeStatus(msg.getRequestId(), msg.getSeqNo(), sender, " line 477");
				QHead = wccInstance.getPendingCommits().get((Integer) sender).getPendQ().peek();
				int sn = msg.getSeqNo();
				sn = sn + 1;
				while (QHead != null && QHead.getSeqNo() == sn) {
					if(QHead!=wccInstance.getPendingCommits().get((Integer) sender).getPendQ().peek()) {
						break;
					}
					if(QHead.getWriteSet()!=null) {
						for (String Id : QHead.getWriteSet().keySet()) {
							if (wccInstance.getStore().Ifreplicate(Id)) {
								wccInstance.getStore().updateStore(QHead.getRequestId(), QHead.getUpdator(),
										Id, QHead.getSeqNo(), QHead.getWriteSet().get(Id)," line 595");
								for(RequestId r: QHead.getCollectedSet()) {
									int siteId= wccInstance.getStore().getLastVersion(Id).getlocalId();
									int sNumber= wccInstance.getStore().getLastVersion(Id).getSeqNo();
									if(wccInstance.getStore().getStore().get(Id).getSitehistory(siteId).addToMainRWINfor(Id, sNumber, r)) {
									}
								}

							}
						}
					}
					wccInstance.getNodeStatus().UpdateNodeStatus(QHead.getRequestId(), QHead.getSeqNo(),QHead.getUpdator(), " line 504");					
					if (QHead.getWriteSet() != null) {
						for (String Id : QHead.getWriteSet().keySet()) {
							if (wccInstance.getStore().Ifreplicate(Id)) {
								wccInstance.getStore().getLastVersion(Id).getObject().setVC(QHead.getCommitVC());
								wccInstance.ReleaseLock(Id, QHead.getUpdator(), QHead.getRequestId(),"Line 323 Finish Commit");
							}
						}
					}
					PendingCommit l=wccInstance.getPendingCommits().get((Integer) sender).getPendQ().poll();
					sn = sn + 1;
					QHead = wccInstance.getPendingCommits().get((Integer) sender).getPendQ().peek();
				}
			}
			else {
				wccInstance.getPendingCommits((Integer)sender).AddToPendQ(new PendingCommit(msg.getRequestId(), msg.getSeqNo(), sender));
			}
		}
	}



	private class MessageHandlerImpl implements MessageHandler {

		public void onMessageReceived(Message msg, int sender) {
			MessageEvent event = new MessageEvent(msg, sender);
			RequestDispatcher.execute(event);
		}
		public void onMessageSent(Message message, BitSet destinations) {
		}

	}

	private class MessageEvent implements Runnable {
		private final Message msg;
		private final int sender;

		public MessageEvent(Message msg, int sender) {
			this.msg = msg;
			this.sender = sender;
		}

		public void run() {
			try {

				switch (msg.getType()) {
				case Prepare:
					onPrepare((Prepare) msg, sender);
					break;
				case PrepareAns:
					onPrepareAns((PrepareAns) msg, sender);
					break;
				case Commit:
					onCommit((Commit) msg, sender);
					break;
				case Abort:
					onAbort((Abort) msg, sender);
					break;
				case Propagate:
					onPropagate((Propagate) msg, sender);
					break;
				default:
					logger.warning("Unknown message type: " + msg);
				}
			} catch (Throwable t) {
				logger.log(Level.SEVERE, "Unexpected exception", t);
				t.printStackTrace();
				System.out.println(
						"The type of exception whichh happened here in twopc is " + t.getMessage() + " from sender "
								+ sender + " for msg type " + msg.getType() + " and requestId " + msg.getRequestId());
				System.exit(-1);
			}
		}
	}

	class NodeStatusUpdator extends Thread {
		Processintwopc p;
		NodeStatusUpdator(Processintwopc p){
			this.p=p;

		}

		public void sendPropagartion(RequestId requestId, int[] startVC,BitSet PropagationDest, int seqNo) throws InterruptedException {
			//Thread.sleep(1);
			if (PropagationDest.cardinality() != 0) {
				byte[] sendVC = wccInstance.serializeVC(startVC);
				Propagate p = new Propagate(requestId, sendVC, seqNo,numNodes);
				network.sendMessage(p, PropagationDest);
			}
		}




		public void run() {
			while(true) {
				try {		
					while(propagationList.size()==0) {}
					PendingCommit pc=propagationList.poll();
					sendPropagartion(pc.getRequestId(), pc.getCommitVC(),pc.getPropagationDestination(), pc.getSeqNo());


				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}


	private final static Logger logger = Logger.getLogger(Processintwopc.class.getCanonicalName());

}