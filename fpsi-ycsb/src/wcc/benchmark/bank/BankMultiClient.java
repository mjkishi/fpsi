package wcc.benchmark.bank;

import java.io.IOException;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;

import wcc.AbortException;
import wcc.Range;
import wcc.common.ProcessDescriptor;
import wcc.common.RequestId;
import wop.TwoPhaseCommit.replica.IdGenerator;
import wop.TwoPhaseCommit.replica.SimpleIdGenerator;


public class BankMultiClient {

	private Vector<ClientThread> clients = new Vector<ClientThread>();
	private final int clientCount;
	private final int requests;
	private final Bank bank;
	private final int readPercentage;
	private int numObjects;

	private int getBalanceCountFinal = 0;
	private int transferCountFinal = 0;
	public double getBalanceLatencyFinal = 0;
	public double transferLatencyFinal = 0;



	class ClientThread extends Thread {
		private final long clientId;
		private int sequenceId = 0;
		private int numObjects;
		private ArrayBlockingQueue<Integer> sends;
		private final Random random;

		public int getBalanceCount = 0;
		public int transferCount = 0;
		public long getBalanceLatency = 0;
		public long transferLatency = 0;



		public ClientThread(long clientId, int numObjects) throws IOException {
			this.clientId = clientId;
			sends = new ArrayBlockingQueue<Integer>(128);
			this.random = new Random();
			this.numObjects = numObjects;

		}


		public void resetCounts() {
			getBalanceCount = 0;
			transferCount = 0;
			getBalanceLatency = 0;
			transferLatency = 0;
		}

		public int getBalanceCount() {
			return getBalanceCount;
		}

		public int gettransferCount() {
			return transferCount;
		}


		@Override
		public void run() {
			try {

				int MinimumIndex=0;
				int MaximumIndex=0;
				Range[] ranges=bank.getWccInstance().getRanges();			
				int localityPercentage=bank.getLoalityPercentage();
				int localId=bank.getWccInstance().getLocalId();	
				Integer count;
				Thread.sleep(5000);/////timeout to start issuing requests. Maybe too short if the system is large
				count = sends.take();
				RequestId requestId;
				int src, dst;
				for (int i = 0; i < count; i++) {
					boolean which;
					if(random.nextInt(100)<localityPercentage) {
						int rangeNo=random.nextInt(2);
						if(rangeNo<ranges.length) {
							MinimumIndex=ranges[rangeNo].getMin();
							MaximumIndex=ranges[rangeNo].getMax();
						}
						else {
							rangeNo=0;
							MinimumIndex=ranges[rangeNo].getMin();
							MaximumIndex=ranges[rangeNo].getMax();

						}	 
						src = random.nextInt(MaximumIndex - MinimumIndex+1) + MinimumIndex;
						dst = random.nextInt(MaximumIndex - MinimumIndex+1) + MinimumIndex;
						while (src == dst) {
							dst = random.nextInt(MaximumIndex - MinimumIndex+1) + MinimumIndex;
						}
						which=true;
					}

					else {	 
						src = random.nextInt(numObjects);
						dst = random.nextInt(numObjects);
						while (src == dst) {
							dst = random.nextInt(numObjects);
						}	
						which=false;
					}		
					requestId = nextRequestId();
					if (random.nextInt(100) < readPercentage) {
						try {
							bank.getBalance(requestId, src, dst);
							bank.getNode().getProcessinRead().sendRemove(requestId);					   
						} catch (AbortException e) {
							System.out.println("A read-only must not get aborted Exitttttttttttttttttttttttttttttt");
							System.exit(-1);
						}
					}

					else {

						try {
							bank.transfer(requestId, src, dst,0);//read -write
							bank.onExecuteComplete(requestId, bank.timeOut, src, dst);//wait mikone
							bank.on2PC(requestId, src, dst);
						}  catch (IOException e) {
							e.printStackTrace();
						}
						catch (InterruptedException e) {
							e.printStackTrace();
						}
					}			    
				}

			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void execute(int count) throws InterruptedException {
			sends.put(count);
		}

		private RequestId nextRequestId() {
			return new RequestId(clientId, ++sequenceId);
		}
	}

	public BankMultiClient(int clientCount, int requests, int numberObjects, int readPercentage, Bank bank) {
		this.bank = bank;
		this.clientCount = clientCount;
		this.requests = requests;
		this.readPercentage = readPercentage;
		this.numObjects = numberObjects;
		bank.initClient(numberObjects, this);
	}

	public void run() throws IOException, InterruptedException {
		execute(this.clientCount, this.requests, numObjects);
	}

	private void execute(int clientCount, int requests, int numObjects) throws IOException, InterruptedException {
		IdGenerator idGenerator = new SimpleIdGenerator(ProcessDescriptor.getInstance().localId,
				ProcessDescriptor.getInstance().numReplicas);
		for (int i = clients.size(); i < clientCount; i++) {
			ClientThread client = new ClientThread(idGenerator.next(), numObjects);
			clients.add(client);
			client.start();
		}
		for (int i = 0; i < clientCount; i++) {
			clients.get(i).execute(requests);
		}
	}

	public void collectLatencyData() {
		for(int i=0; i<clients.size(); i++) {
			this.getBalanceCountFinal += clients.get(i).getBalanceCount;
			this.transferCountFinal += clients.get(i).transferCount;
			this.getBalanceLatencyFinal += clients.get(i).getBalanceLatency;
			this.transferLatencyFinal += clients.get(i).transferLatency;
			clients.get(i).getBalanceCount = 0;
			clients.get(i).transferCount = 0;
			clients.get(i).getBalanceLatency = 0;
			clients.get(i).transferLatency = 0;
		}
		this.getBalanceLatencyFinal = this.getBalanceLatencyFinal / this.getBalanceCountFinal;
		this.transferLatencyFinal = this.transferLatencyFinal / this.transferCountFinal; 
	}

	public double getBalanceLatency() {
		double latency = this.getBalanceLatencyFinal;
		this.getBalanceLatencyFinal = 0;
		this.getBalanceCountFinal = 0;
		return latency;
	}

	public double gettransferLatency() {
		double latency = this.transferLatencyFinal;
		this.transferLatencyFinal = 0;
		this.transferCountFinal = 0;
		return latency;	
	}

	public int gerRequestNumber() {
		return this.requests;
	}


}
