package wcc.benchmark.tpcc;

import java.io.IOException;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;

import wcc.Range;
import wop.TwoPhaseCommit.replica.IdGenerator;
import wop.TwoPhaseCommit.replica.SimpleIdGenerator;
import wcc.common.ProcessDescriptor;
import wcc.common.RequestId;


public class TpccMultiClient {
	private Vector<ClientThread> clients = new Vector<ClientThread>();
	private final int clientCount;
	private final int requests;
	private final Tpcc tpcc;
	private final int readPercentage;
	private final boolean tpccProfile;

	private int readCountFinal = 0;
	private int writeCountFinal = 0;
	public double readLatencyFinal = 0;
	public double writeLatencyFinal = 0;


	class ClientThread extends Thread {
		private final long clientId;
		private int sequenceId = 0;
		private ArrayBlockingQueue<Integer> sends;
		private final Random random;

		public int readCount = 0;
		public int writeCount = 0;
		public long readLatency = 0;
		public long writeLatency = 0;



		private int stockLevelPercentage;
		private int orderStatusPercentage;
		private int newOrderPercentage;
		private int paymentPercentage;
		private int deliveryPercentage;



		public ClientThread(long clientId, int orderStatusPercentage, int stockLevelPercentage, 
				int neworderPercentage,int paymentPercentage, int deliveryPercentage) throws IOException {
			this.clientId = clientId;
			sends = new ArrayBlockingQueue<Integer>(128);
			this.random = new Random();
			this.deliveryPercentage=deliveryPercentage;
			this.orderStatusPercentage=orderStatusPercentage;
			this.newOrderPercentage=neworderPercentage;
			this.paymentPercentage=paymentPercentage;
			this.stockLevelPercentage=stockLevelPercentage;
		}

		public void resetCounts() {
			readCount = 0;
			writeCount = 0;
			readLatency = 0;
			writeLatency = 0;
		}

		@Override
		public void run() {
			try {
				int minimumIndex=0;
				int maximumIndex=0;
				int wNumber=0;
				Range[] ranges=tpcc.getDssInstance().getRanges();
				int localityPercentage=tpcc.getLocalityPercentage();
				boolean tpccProfile=tpcc.getTpccProfile();
				Integer count;
				Thread.sleep(6000);
				count = sends.take();
				RequestId requestId;
				for (int i = 0; i < count; i++) {
					if(random.nextInt(100)<localityPercentage) {
						int rangeNo=random.nextInt(2);
						if(rangeNo<ranges.length) {
							minimumIndex=ranges[rangeNo].getMin();
							maximumIndex=ranges[rangeNo].getMax();
							wNumber=random.nextInt(maximumIndex - minimumIndex+1) + minimumIndex;
						}
						else {
							rangeNo=0;
							minimumIndex=ranges[rangeNo].getMin();
							maximumIndex=ranges[rangeNo].getMax();
							wNumber=random.nextInt(maximumIndex - minimumIndex+1) + minimumIndex;
						}
					}
					else {
						minimumIndex=0;
						maximumIndex=tpcc.NUM_WAREHOUSES;
						wNumber=random.nextInt(maximumIndex - minimumIndex) + minimumIndex;
					}

					requestId =nextRequestId(); 
					int readOnlyPercentage=this.stockLevelPercentage+this.orderStatusPercentage;
					int txType=random.nextInt(100);
					if(txType<this.stockLevelPercentage) {
						final int  my_id=wNumber;
						final int o_myid=random.nextInt(tpcc.NUM_ORDERS_PER_D);
						tpcc.stockLevel(requestId,my_id,o_myid, -1);
						tpcc.getNode().getProcessinRead().sendRemove(requestId);
					}
					else if(txType >=this.stockLevelPercentage && txType<readOnlyPercentage) {
						final int w_id=wNumber;
						final int c_myid=random.nextInt(tpcc.NUM_CUSTOMERS_PER_D);
						final int o_myid=random.nextInt(tpcc.NUM_ORDERS_PER_D);
						tpcc.orderStatus(requestId, w_id,c_myid,o_myid,-1);	
						tpcc.getNode().getProcessinRead().sendRemove(requestId);
					}				
					else if(txType >=readOnlyPercentage && txType<readOnlyPercentage+this.newOrderPercentage) {
						if(!tpccProfile) {
							final int w_id = wNumber;
							final int d_id = random.nextInt(tpcc.NUM_DISTRICTS);
							final int c_id = random.nextInt(tpcc.NUM_CUSTOMERS_PER_D);
							final int o_carrier_id=random.nextInt(15);
							final int o_myid=random.nextInt(tpcc.NUM_ORDERS_PER_D);
							final int i_id = random.nextInt((maximumIndex - minimumIndex+1)) + minimumIndex;				
							final int ol_myid=random.nextInt(1000) + tpcc.NUM_ORDERS_PER_D;
							final int ol_quantity=random.nextInt(1000); 
							tpcc.newOrder(requestId, w_id, d_id, c_id, o_carrier_id, o_myid, i_id, ol_myid, ol_quantity,0);
							tpcc.onExecuteComplete(requestId, tpcc.timeOut);
							tpcc.on2PC(requestId, 3, w_id, d_id, c_id, o_carrier_id, o_myid, i_id, ol_myid, ol_quantity,0, tpcc.getTpccProfile());
						}
						else {
							int tpccProfileLocality=random.nextInt(100);
							if(tpccProfileLocality<90) {
								int rangeNo=random.nextInt(2);
								if(rangeNo<ranges.length) {
									minimumIndex=ranges[rangeNo].getMin();
									maximumIndex=ranges[rangeNo].getMax();
									wNumber=random.nextInt(maximumIndex - minimumIndex+1) + minimumIndex;
								}
								else {
									rangeNo=0;
									minimumIndex=ranges[rangeNo].getMin();
									maximumIndex=ranges[rangeNo].getMax();
									wNumber=random.nextInt(maximumIndex - minimumIndex+1) + minimumIndex;
								}		
								final int w_id = wNumber;
								final int d_id = random.nextInt(tpcc.NUM_DISTRICTS);
								final int c_id = random.nextInt(tpcc.NUM_CUSTOMERS_PER_D);
								final int o_carrier_id=random.nextInt(15);
								final int o_myid=random.nextInt(tpcc.NUM_ORDERS_PER_D);
								final int i_id = random.nextInt((maximumIndex - minimumIndex)) + minimumIndex;
								final int ol_myid=random.nextInt(1000) + tpcc.NUM_ORDERS_PER_D;
								final int ol_quantity=random.nextInt(1000); 
								tpcc.newOrder(requestId, w_id, d_id, c_id, o_carrier_id, o_myid, i_id, ol_myid, ol_quantity,0);
								tpcc.onExecuteComplete(requestId, tpcc.timeOut);
								tpcc.on2PC(requestId, 3, w_id, d_id, c_id, o_carrier_id, o_myid, i_id, ol_myid, ol_quantity,0, tpcc.getTpccProfile());
							}
							else {
								int w_id=0;
								while(true) {
									w_id=random.nextInt(tpcc.NUM_WAREHOUSES);
									String warehouseId="w_"+ Integer.toString(w_id);
									if(!tpcc.getDssInstance().IfReplicate(warehouseId)) {
										break;
									}
								}
								final int d_id = random.nextInt(tpcc.NUM_DISTRICTS);
								final int c_id = random.nextInt(tpcc.NUM_CUSTOMERS_PER_D);
								final int o_carrier_id=random.nextInt(15);
								final int o_myid=random.nextInt(tpcc.NUM_ORDERS_PER_D);
								final int i_id = random.nextInt((maximumIndex - minimumIndex)) + minimumIndex;
								final int ol_myid=random.nextInt(1000) + tpcc.NUM_ORDERS_PER_D;
								final int ol_quantity=random.nextInt(1000); 
								tpcc.newOrder(requestId, w_id, d_id, c_id, o_carrier_id, o_myid, i_id, ol_myid, ol_quantity,0);
								tpcc.onExecuteComplete(requestId, tpcc.timeOut);
								tpcc.on2PC(requestId, 3, w_id, d_id, c_id, o_carrier_id, o_myid, i_id, ol_myid, ol_quantity,0, tpcc.getTpccProfile());
							}
						}

					}

					else if(txType>=readOnlyPercentage+this.newOrderPercentage && txType<readOnlyPercentage+this.newOrderPercentage+this.paymentPercentage) {
						final float h_amount = (float) (random.nextInt(500000) * 0.01);
						final int w_id = wNumber;
						final int c_id = random.nextInt(tpcc.NUM_CUSTOMERS_PER_D);
						final int d_id = random.nextInt(tpcc.NUM_DISTRICTS);
						tpcc.payment(requestId,h_amount,w_id,c_id,d_id, 0, tpcc.getTpccProfile());
						tpcc.onExecuteComplete(requestId, tpcc.timeOut);
						tpcc.on2PC(requestId, 5, w_id, c_id, d_id, 0, 0, 0, 0, 0, h_amount, tpcc.getTpccProfile());
					}						
					else if(txType>=readOnlyPercentage+this.newOrderPercentage+this.paymentPercentage)  {
						final int my_id=wNumber;
						final int o_myid=random.nextInt(tpcc.NUM_ORDERS_PER_D);
						final int c_myid=random.nextInt(tpcc.NUM_CUSTOMERS_PER_D);
						tpcc.delivery(requestId,my_id, o_myid, c_myid, 0);
						tpcc.onExecuteComplete(requestId, tpcc.timeOut);
						tpcc.on2PC(requestId, 4, my_id, o_myid, c_myid, 0, 0, 0, 0, 0, 0, tpcc.getTpccProfile());
					}
					else {
						System.out.println("Not-determined!!"+" txType is "+txType);
						System.exit(-1);
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

	public TpccMultiClient(int clientCount, int requests, int readPercentage,
			boolean tpccProfile, Tpcc tpcc) {
		this.tpcc = tpcc;
		this.clientCount = clientCount;
		this.requests = requests;
		this.readPercentage = readPercentage;
		this.tpccProfile = tpccProfile;
		tpcc.initClient(this);
	}

	public void run(int orderStatusPercentage, int stockLevelPercentage, 
			int neworderPercentage,int paymentPercentage, int deliveryPercentage) throws IOException,
	InterruptedException {

		execute(this.clientCount, this.requests,tpcc.NUM_ITEMS, orderStatusPercentage, stockLevelPercentage, 
				neworderPercentage, paymentPercentage,  deliveryPercentage);
	}

	private void execute(int clientCount, int requests, int numObjects, int orderStatusPercentage, int stockLevelPercentage, int neworderPercentage,int paymentPercentage, int deliveryPercentage) throws IOException, InterruptedException {
		IdGenerator idGenerator = new SimpleIdGenerator(
		ProcessDescriptor.getInstance().localId,
		ProcessDescriptor.getInstance().numReplicas);
		for (int i = 0; i < clientCount; i++) {
			ClientThread client = new ClientThread(idGenerator.next(),orderStatusPercentage, stockLevelPercentage, neworderPercentage, paymentPercentage,  deliveryPercentage);
			clients.add(client);
			client.start();
		}
		for (int i = 0; i < clientCount; i++) {
			clients.get(i).execute(requests);
		}
	}

	public void collectLatencyData() {
		for(int i=0; i<clients.size(); i++) {
			this.readCountFinal += clients.get(i).readCount;
			this.writeCountFinal += clients.get(i).writeCount;
			this.readLatencyFinal += clients.get(i).readLatency;
			this.writeLatencyFinal += clients.get(i).writeLatency;
			clients.get(i).readCount = 0;
			clients.get(i).writeCount = 0;
			clients.get(i).readLatency = 0;
			clients.get(i).writeLatency = 0;
		}
		this.readLatencyFinal = this.readLatencyFinal / this.readCountFinal;
		this.writeLatencyFinal = this.writeLatencyFinal / this.writeCountFinal; 
	}

	public double getReadLatency() {
		double latency = this.readLatencyFinal;
		this.readLatencyFinal = 0;
		this.readCountFinal = 0;
		return latency;
	}


	public double getWriteLatency() {
		double latency = this.writeLatencyFinal;
		this.writeLatencyFinal = 0;
		this.writeCountFinal = 0;
		return latency;	
	}



}
