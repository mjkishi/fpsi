package wop.TwoPhaseCommit;

import java.io.IOException;
import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import wcc.ReadResponse;
import wcc.Wcc;
import wcc.common.RequestId;
import wop.network.TcpNetwork;
import wop.transaction.AbstractObject;
import wop.transaction.ReadSetEntry;
import wop.transaction.TransactionContext;
import wop.messages.Message;
import wop.messages.MessageType;
import wop.messages.Read;
import wop.messages.ReadAns;
import wop.messages.Remove;
import wop.network.MessageHandler;
import wop.network.Network;




public class ProcessinRead {
	private TcpNetwork network;
	private Wcc wccInstance;
	int localId;
	private ConcurrentHashMap<RequestId, ReadRequest> RemoteReadMap;
	public int numNodes;
	private static ExecutorService ReadRequestDispatcher;



	public ProcessinRead(Wcc wccInstance, int localId, int numNodes) throws IOException {
		this.wccInstance = wccInstance;
		this.localId = localId;
		RemoteReadMap = new ConcurrentHashMap<RequestId, ReadRequest>();
		this.numNodes = numNodes;
		this.network=new TcpNetwork(1);		
	}

	public void startProcessforRead() throws IOException {
		MessageHandler handler = new MessageHandlerImpl();
		Network.addMessageListener(MessageType.Read, handler);
		Network.addMessageListener(MessageType.ReadAns, handler);
		Network.addMessageListener(MessageType.Remove, handler);
		ReadRequestDispatcher = Executors.newFixedThreadPool(20);
		network.start();
	}

	public Wcc getwccInstance() {
		return wccInstance;
	}

	public int getLocalId() {
		return localId;
	}

	public TcpNetwork getNetwork() {
		return network;
	}

	public int getnumNodes() {
		return this.numNodes;
	}


	public void CreateReadReq(RequestId requestId) {
		if (!RemoteReadMap.containsKey(requestId)) {
			ReadRequest readreq = new ReadRequest();
			RemoteReadMap.put(requestId, readreq);
		}

	}

	public void removeReadReq(RequestId requestId) {
		if (RemoteReadMap.containsKey(requestId)) {
			RemoteReadMap.remove(requestId);
		}
	}


	public void SendRead(Read msg, BitSet Destination) {
		RequestId requestId = msg.getRequestId();
		CreateReadReq(requestId);
		ReadRequest readreq = RemoteReadMap.get(requestId);
		readreq.AddToIdList(msg.getObjectId());
		RemoteReadMap.put(requestId, readreq);
		network.sendMessage(msg, Destination);
	}



	public void onRead(Read msg, int sender) throws IOException{ 
		int[] vc=wccInstance.deserializeVC(msg.getVC());
		int[] hasRead=wccInstance.deserializehasRead(msg.gethasRead());
		ReadResponse response=wccInstance.getStore().getLatestObject1(msg.getRequestId(), msg.getObjectId(), vc,hasRead,  this.numNodes, msg.getIsUpdate(),sender);
		byte[] objByte=wccInstance.getSTMService().serializeObject(response.getObject());
		byte[] maxVCByte=wccInstance.serializeVC(response.getSuggestedVC());		 	 
		ReadAns m=new ReadAns(msg,objByte.length,objByte,maxVCByte ,this.numNodes,msg.getRetryTimes(),msg.getIsUpdate(), response.getSiteId(), response.getObjSeqNo());
		network.sendMessage(m, sender);
	}	


	public void onReadAns(ReadAns msg, int sender){
		byte[] obj=msg.getObj();
		AbstractObject object=wccInstance.getSTMService().deserializeObject(msg.getObjectId(),obj);
		int[]  maxVC= wccInstance.deserializeVC(msg.getStartVTS());
		wccInstance.notifyforRead(msg.getRequestId(),msg.getObjectId() ,object,maxVC,sender,msg.getRetryTimes(), msg.getSiteId(), msg.getObjSeqNo());
	}






	public void  onRemove(Remove msg, int sender) {
		int siteId=msg.getSiteId();
		int objSeqNo=msg.getObjSeqNo();
		String objId=msg.getObjectId();
		while(!wccInstance.getStore().getStore().get(objId).getSitehistory(siteId).getMainRWInfo(objId, objSeqNo).remove(msg.getRequestId()));{}
		for(int i=0; i< this.numNodes;i++) {
			int seqNo=wccInstance.getStore().getStore().get(objId).getSitehistory(i).getLatestSeqNo();
			synchronized (wccInstance.getStore().getStore().get(objId).getSitehistory(i).getMainRWInfo(objId, seqNo)) {
				if(wccInstance.getStore().getStore().get(objId).getSitehistory(i).getMainRWInfo(objId, seqNo).remove(msg.getRequestId())) {
				}
			}
		}

	}

	public void sendRemove(RequestId requestId) {			
		Map<String , ReadSetEntry> rs=wccInstance.getlocalTransactionContext(requestId).getReadSet();
		for(String key:rs.keySet() ) {
			BitSet destKey=new BitSet(0);
			ReadSetEntry entry=rs.get(key);
			destKey=wccInstance.getDestination(key);
			Remove remove=new Remove(requestId, entry.getSiteId(), entry.getObjectSeqNo(), key.length(), key);
			network.sendMessage(remove, destKey.nextSetBit(0));
		}
		removeReadReq(requestId);
		wccInstance.removeTransactionContext(requestId);		
	}



	/**
	 * Receives messages from other processes.
	 */
	 private class MessageHandlerImpl implements MessageHandler {
		 public void onMessageReceived(Message msg, int sender) {
			 MessageEvent event = new MessageEvent(msg, sender);
			 ReadRequestDispatcher.execute(event);
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
				 case Read:
					 onRead((Read) msg, sender);
					 break;

				 case ReadAns:
					 ReadAns readans = (ReadAns) msg;
					 ReadRequest readreq = RemoteReadMap.get(msg.getRequestId());					
					 TransactionContext ctx=wccInstance.getlocalTransactionContext(msg.getRequestId());
					 if (ctx !=null && readreq !=null    ) {
						 if(readans.getRetryTimes()==ctx.getRetryTimes()) {
							 if(readreq.FisrtResponse(readans.getObjectId())) {
								 onReadAns((ReadAns) msg, sender);
							 }
						 }

					 }		
					 break;
				 case Remove:
					 Remove remove = (Remove) msg;
					 onRemove(remove,sender);
					 break;

				 default:
					 logger.warning("Unknown message type: " + msg);
				 }
			 } catch (Throwable t) {
				 logger.log(Level.SEVERE, "Unexpected exception", t);
				 System.out.println("The type of exception whichh happened here in Read is for requestId "
						 + msg.getRequestId() + " from sender " + sender);
				 t.printStackTrace();
				 System.out.println(msg.getType());
				 System.exit(1);
			 }
		 }
	 }

	 /***************************************************/
	 private final static Logger logger = Logger.getLogger(ProcessinRead.class.getCanonicalName());



}
