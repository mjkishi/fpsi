package wcc.benchmark.bank;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import wcc.AbortException;
import wcc.Store;
import wcc.Wcc;
import wcc.common.ProcessDescriptor;
import wcc.common.RequestId;
import wop.TwoPhaseCommit.replica.Node;
import wop.service.STMService;
import wop.transaction.AbstractObject;
import wop.transaction.TransactionContext;

public class Bank extends STMService{
	protected final int INITIAL_BALANCE = 100;
	protected final int DEFAULT_TRANSACTION_AMOUNT = 10;
	String ACCOUNT_PREFIX = "account_";
	public long timeOut; 
	private AtomicInteger readCount= new AtomicInteger(0);
	private AtomicInteger committedCount= new AtomicInteger(0);
	private AtomicInteger abortedCount= new AtomicInteger(1);
	private int localId;
	private int numRequests;
	int LocalityPercentage;
	public static int numNodes;  
	private long startRead;
	private long endRead;
	private long lastReadCount = 0;
	private long lastWriteCount = 0;
	private long lastAbortCount = 0;	
	protected int numAccounts;
	Store store;
	Wcc wccInstance;
	Node replica;
	BankMultiClient client;

	public AtomicInteger avgSizeCollected=new AtomicInteger(0);

	Bank(int numNodes){
		Bank.numNodes=numNodes;
	}


	MonitorThread monitorTh = new MonitorThread();



	class MonitorThread extends Thread {

		public void run() {
			long start;
			long count = 0;
			long localReadCount = 0;
			long localWriteCount = 0;
			long localAbortCount = 0;


			System.out
			.println("Read-Throughput/S  Write Throughput/S  Latency Aborts Time");
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			while (true) {
				startRead = System.currentTimeMillis();

				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				localReadCount = readCount.get();
				localWriteCount = committedCount.get();
				localAbortCount =abortedCount.get();
				endRead = System.currentTimeMillis();
				//client.collectLatencyData();
				System.out.format("%5d  %5d  %5d \n",
						((localReadCount - lastReadCount) * 1000)
						/ (endRead - startRead),
						((localWriteCount - lastWriteCount) * 1000)
						/ (endRead - startRead),
						((localAbortCount - lastAbortCount) * 1000)
						/ (endRead - startRead));
				long denominator=(localWriteCount - lastWriteCount+1);
				lastAbortCount = localAbortCount;
				lastReadCount = localReadCount;
				lastWriteCount = localWriteCount;



				int sum=committedCount.get()+readCount.get();
				float abortRate=(float)((float)abortedCount.get()/(float)(committedCount.get()+abortedCount.get()));
				System.out.println("Total number of requests "+sum +" Committed Count"+committedCount+ 
						" Aborted Count "+abortedCount+ " Read Count "+readCount+ "And abort rate percentage is "+abortRate+" and average size for collected set is "+
						avgSizeCollected.get()/denominator);

				avgSizeCollected.set(0);
				count++;
			}
		}
	}





	public void init(int numRequests,int numAccounts, Store store, Wcc wccinstance, int numNodes,int localId, int localityPercentage ) { 
		this.localId=localId;
		this.numRequests= numRequests;
		this.store=store;
		this.numAccounts = numAccounts;
		this.LocalityPercentage=localityPercentage;        
		this.wccInstance = wccinstance;
		store.registerPartitions(numNodes,localId);
		for (int i = 0; i < this.numAccounts; i++) {
			String accountId = ACCOUNT_PREFIX + Integer.toString(i);
			Account account = new Account(INITIAL_BALANCE, accountId);
			this.store.registerObjectToStoreForLocality(accountId,account,i, localId,wccInstance.getNumNodes());
		}
		this.monitorTh.start();	        
	}

	public void init() {
		this.localId = ProcessDescriptor.getInstance().localId;
	}




	public void getBalance(RequestId requestId, int src, int dst) throws IOException, AbortException,  InterruptedException{
		Account srcAccount, dstAccount;
		String srcId = ACCOUNT_PREFIX + Integer.toString(src);
		String dstId = ACCOUNT_PREFIX + Integer.toString(dst);
		srcAccount=(Account) wccInstance.Read(srcId, "r", requestId,-1);
		dstAccount = (Account) wccInstance.Read(dstId, "r", requestId,-1);
		int balance = srcAccount.getAmount() + dstAccount.getAmount();
		readCount.incrementAndGet();
		return; 
	}



	public void transfer(RequestId requestId, int src, int dst, int retryTimes) throws IOException/*, InterruptedException*/,InterruptedException {
		String srcId = ACCOUNT_PREFIX + Integer.toString(src);
		String dstId = ACCOUNT_PREFIX + Integer.toString(dst);
		Account srcAccount, dstAccount;
		Account acc1=(Account) wccInstance.Read(srcId, "rw", requestId,retryTimes);
		Account acc2=(Account) wccInstance.Read(dstId, "rw", requestId,retryTimes);
		srcAccount=new Account(acc1.getAmount(), acc1.getId());
		dstAccount=new Account(acc2.getAmount(), acc2.getId());
		srcAccount.withdraw(DEFAULT_TRANSACTION_AMOUNT); // Modify the copy
		dstAccount.deposit(DEFAULT_TRANSACTION_AMOUNT); // Modify the copy
		srcAccount= ((Account) wccInstance.Write(srcId, srcAccount, requestId,retryTimes));
		dstAccount=((Account) wccInstance.Write(dstId,dstAccount, requestId,retryTimes));
	}




	public void initClient(int numAccounts, BankMultiClient client) {
		this.numAccounts = numAccounts;
		this.client = client;
	}


	public void setNode(Node replica) {
		this.replica = replica;
	}





	public void rollback1(RequestId requestId, int src, int dst, int retryTimes){
		wccInstance.removeTransactionContext(requestId);
		getNode().getProcessinRead().removeReadReq(requestId);
		getNode().getProcessintwopc().removeTwopcSample(requestId);
		try {
			transfer(requestId, src, dst,retryTimes);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}






	public void onExecuteComplete(RequestId requestId, long timeOut, int src, int dst) throws InterruptedException, IOException {
		getNode().getProcessintwopc().Prepare(requestId);
		TransactionContext ctx=wccInstance.getlocalTransactionContext(requestId);
		while(!ctx.getReady()) {
		}   
	}


	public void on2PC(RequestId requestId, int src, int dst) throws InterruptedException, IOException {
		while(wccInstance.getlocalTransactionContext(requestId).getRetry() ) {
			abortedCount.incrementAndGet();
			int retryT=wccInstance.getlocalTransactionContext(requestId).incrementRetryTimes();
			rollback1(requestId, src, dst,retryT);
			onExecuteComplete(requestId,timeOut, src, dst);
		}
		wccInstance.getlocalTransactionContext(requestId).setReadyToRemove();
	}



	public byte[] serializeObject(AbstractObject object) throws IOException{
		ByteBuffer bb;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		if (object instanceof Account) {
			bb = ByteBuffer.allocate(8+4);
			Account account=(Account) object;
			bb.putInt(account.getAmount());
			bb.putLong(account.getVersion());
			bb.flip();
			out.write(bb.array());
		}
		else {
			System.out.println("Bank Object serialization: object not defined");
		}   
		return out.toByteArray();
	}


	public AbstractObject deserializeObject(byte[] bytes){
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		int amount=bb.getInt();
		long version=bb.getLong();
		Account object=new Account();
		object.setAmount(amount);
		object.setVersion(version);
		AbstractObject outObj=(AbstractObject)object;
		return outObj;
	}


	public byte[] serializeWriteSet(TransactionContext ctx) {
		Map<String, AbstractObject> writeset=ctx.getWriteSet();
		ByteBuffer bb = ByteBuffer.allocate(writeset.size() * 16);
		for (Map.Entry<String, AbstractObject> entry : writeset.entrySet()) {
			String id = entry.getKey();
			id = id.replace("account_", "");
			Account account = (Account) entry.getValue();
			bb.putInt(Integer.parseInt(id));
			bb.putLong(account.getVersion());
			bb.putInt(account.getAmount());
		}
		return bb.array();
	}



	public Map<String,AbstractObject> deserializeWriteSet(int wsize,byte[] bytes){
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		Map<String,AbstractObject>writeset=new HashMap<String, AbstractObject>();
		for (int i = 0; i < wsize; i++) {
			String id = "account_" + bb.getInt();
			long version = bb.getLong();
			int value = bb.getInt();
			Account account = new Account();
			account.setId(id);
			account.setVersion(version);
			account.setAmount(value);
			writeset.put(id, account);
		}
		return writeset;    
	}


	public Node getNode() {
		return this.replica;
	}

	public Wcc getWccInstance() {
		return this.wccInstance;
	}

	public int getAmount(AbstractObject obj){
		Account account=(Account)(obj);
		return account.getAmount();
	}

	public void incrementAbortedCount() {
		abortedCount.incrementAndGet(); 
	}

	@Override
	public void IncreaseCommitCount() {
		this.committedCount.incrementAndGet();			
	}

	public int getLoalityPercentage() {
		return this.LocalityPercentage;
	}

	public synchronized void addToCollectedNum(int collectedNum) {
		this.avgSizeCollected.addAndGet(collectedNum);
	}

	public void resetCollectedNum() {
		this.avgSizeCollected.set(0);
	}
}
