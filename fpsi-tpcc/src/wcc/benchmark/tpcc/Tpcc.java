package wcc.benchmark.tpcc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import wcc.Wcc;
import wcc.Store;
import wop.transaction.AbstractObject;
import wop.transaction.ReadSetEntry;
import wop.transaction.TransactionContext;
import wop.TwoPhaseCommit.replica.Node;
import wcc.common.ProcessDescriptor;
import wcc.common.RequestId;

import wop.service.STMService;


public class Tpcc extends STMService {
	public long timeOut; 
	Store store;
	Wcc dssInstance;
	Node replica;
	TpccMultiClient client;
	private int localityPercentage;
	private boolean tpccProfile;
	public static int numNodes;  

	
	public int NUM_ITEMS ; 
	public int NUM_WAREHOUSES ;
	public  int NUM_DISTRICTS ; 
	public  int NUM_CUSTOMERS_PER_D ;
	public  int NUM_ORDERS_PER_D ; 

	/** data collector variables **/
	static long startRead;
	static long startWrite;
	static long endRead;
	static long endWrite;


	private long lastReadCount = 0;
	private long lastWriteCount = 0;
	private long lastAbortCount = 0;

	private int localId;
	private AtomicInteger committedCount= new AtomicInteger(0);
	private AtomicInteger abortedCount= new AtomicInteger(1);
	private AtomicInteger readCount= new AtomicInteger(0);
	
	
	
	
	
	Random random=new Random();

	MonitorThread monitorTh = new MonitorThread();

	
	Tpcc(int numNodes){
		Tpcc.numNodes=numNodes;
	}
	

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
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			while (true) {
				startRead = System.currentTimeMillis();

				try {
					Thread.sleep(10000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
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
				lastAbortCount = localAbortCount;
				lastReadCount = localReadCount;
				lastWriteCount = localWriteCount;
				

				int sum=committedCount.get()+readCount.get();
				float abortRate=(float)((float)abortedCount.get()/(float)(committedCount.get()+abortedCount.get()));
				System.out.println("Total number of requests "+sum +" Committed Count"+committedCount+ 
						" Aborted Count "+abortedCount+ " Read Count "+readCount+ "And abort rate percentage is "+abortRate);
				
				count++;
			}
		}
	} 



	public void TpccInit(Store store,
			Wcc dssinstance,int numRequests, int localId, int numNodes, int warehouseCount, int itemCount, int localityPercentage, long timeOut, boolean tpccProfile) {
		this.NUM_WAREHOUSES = warehouseCount;//c
		this.NUM_ITEMS = 1250;
		this.NUM_DISTRICTS=10;
		this.NUM_CUSTOMERS_PER_D=100;
		this.NUM_ORDERS_PER_D=100;
		
		
		this.store = store;
		this.dssInstance = dssinstance;
		this.localityPercentage=localityPercentage;
		this.timeOut=timeOut;
		this.tpccProfile=tpccProfile;
		store.registerPartitions(numNodes, localId);
		for (int id = 0; id < NUM_ITEMS; id++) {//c
			final String myid = "i_" + Integer.toString(id);
			TpccItem item = new TpccItem(myid);
			this.store.registerObjectToStoreForLocality(myid, item, id, localId, numNodes,NUM_ITEMS);
		}
		for (int id = 0; id < NUM_WAREHOUSES; id++) {//c
			final String myid = "w_" + Integer.toString(id);
			TpccWarehouse warehouse = new TpccWarehouse(myid);
			store.registerObjectToStoreForLocality(myid, warehouse, id, localId, numNodes,NUM_WAREHOUSES);
			for (int s_id = 0; s_id < NUM_ITEMS; s_id++) {//c
				final String smyid = myid + "_s_" + Integer.toString(s_id);
				TpccStock stock = new TpccStock(smyid);
				this.store.registerToStore(smyid, myid, stock,localId,numNodes);
			}    
			for (int d_id = 0; d_id < NUM_DISTRICTS; d_id++) {
				String dmyid = myid + "_" + Integer.toString(d_id);
				TpccDistrict district = new TpccDistrict(dmyid);
				store.registerToStore(dmyid, myid, district,localId,numNodes);
				for (int c_id = 0; c_id < NUM_CUSTOMERS_PER_D; c_id++) {
					String cmyid = myid + "_c_" + Integer.toString(c_id);
					TpccCustomer customer = new TpccCustomer(cmyid);
					this.store.registerToStore(cmyid, myid, customer, localId, numNodes);
				}  
				for (int o_id = 0; o_id < NUM_ORDERS_PER_D; o_id++) {
					String omyid = myid + "_o_" + Integer.toString(o_id);
					TpccOrder order = new TpccOrder(omyid);
					this.store.registerToStore(omyid, myid, order, localId, numNodes);
					String olmyid = myid + "_ol_" + Integer.toString(o_id);
					TpccOrderline orderLine = new TpccOrderline(olmyid);
					this.store.registerToStore(olmyid, myid, orderLine, localId, numNodes);
				}
			}
		}
		this.monitorTh.start();
	}





	public void initRequests() {
		this.localId = ProcessDescriptor.getInstance().localId;
	}

	public void initClient(TpccMultiClient client) {
		this.client = client;
	}



	protected void orderStatus(RequestId requestId , int w_id,int c_myid, int o_myid,  int retryTimes) throws IOException, InterruptedException {
		String myid = "w_"+ Integer.toString(w_id);
		String cmyid = myid + "_c_"+ Integer.toString(c_myid);
		TpccWarehouse warehouse = ((TpccWarehouse) dssInstance.Read(myid, "r",requestId, retryTimes));
		TpccCustomer customer = ((TpccCustomer) dssInstance.Read(cmyid, "r",requestId, retryTimes));
		final String omyid = myid + "_o_"+ Integer.toString(o_myid);
		TpccOrder order = ((TpccOrder) dssInstance.Read(omyid, "r",requestId, retryTimes));
		float olsum = (float) 0;
		int i = 1;
		while (i < order.O_OL_CNT) {
			final String olmyid = myid + "_ol_" + Integer.toString(i);
			TpccOrderline orderline = ((TpccOrderline) dssInstance.Read(olmyid, "r",requestId, retryTimes));
			if (orderline != null) {
				olsum += orderline.OL_AMOUNT;
				i += 1;
			}
		}
		this.readCount.incrementAndGet();
	}

	
	protected void delivery(RequestId requestId , int my_id, int o_myid, int c_myid, int retryTimes) throws IOException, InterruptedException{
		final String myid = "w_"+ Integer.toString(my_id);
		TpccWarehouse warehouse = ((TpccWarehouse) dssInstance.Read(myid, "rw",requestId, retryTimes));
		for (int d_id = 0; d_id < NUM_DISTRICTS; d_id++) {
			final String omyid = myid + "_o_"+ Integer.toString(o_myid);
			final String cmyid = myid + "_c_"+ Integer.toString(c_myid);
			TpccOrder order = ((TpccOrder) dssInstance.Read(omyid, "rw", requestId,retryTimes ));
			float olsum = (float) 0;
			String crtdate = new java.util.Date().toString();
			int i = 1;
			while (i < order.O_OL_CNT) {
				if (i < NUM_ORDERS_PER_D) {
					final String olmyid = myid + "_ol_" + Integer.toString(i);
					TpccOrderline orderline = ((TpccOrderline) dssInstance.Read(olmyid, "rw", requestId, retryTimes));
					if (orderline != null) {
						olsum += orderline.OL_AMOUNT;
						i += 1;
					}
				}
			}
			TpccCustomer customer = ((TpccCustomer) dssInstance.Read(cmyid, "rw",requestId,retryTimes));
			TpccCustomer newCustomer=customer.deepcopy();
			newCustomer.C_BALANCE += olsum;
			newCustomer.C_DELIVERY_CNT += 1;
			newCustomer=((TpccCustomer) dssInstance.Write(cmyid, newCustomer, requestId,retryTimes ));
		}
	}

	protected void stockLevel(RequestId requestId , int my_id,int o_myid,int retryTimes) throws IOException, InterruptedException {
		int i = 0;
		final String myid = "w_"+ Integer.toString(my_id);
		final String omyid = myid + "_o_"+ Integer.toString(o_myid);
		TpccWarehouse warehouse = ((TpccWarehouse) dssInstance.Read(myid, "r", requestId, retryTimes ));
		TpccOrder order = ((TpccOrder) dssInstance.Read(omyid, "r", requestId, retryTimes));
		while (i < 20) {
			if (order != null) {
				int j = 1;
				while (j < order.O_OL_CNT) {
					if (j < NUM_ORDERS_PER_D) {
						final String olmyid = myid + "_ol_"+ Integer.toString(j);
						TpccOrderline orderline = ((TpccOrderline) dssInstance.Read(olmyid, "r",requestId, retryTimes));
					}
					j += 1;
				}
			}
			i += 1;
		}		
		int k = 1;
		while (k <= 10) {
			String wid = "w_"+ Integer.toString(my_id);
			if (k < NUM_ITEMS) {
				String smyid = wid + "_s_" + Integer.toString(k);
				TpccStock stock = ((TpccStock) dssInstance.Read(smyid, "r",requestId, retryTimes));
				k += 1;
			} else
				k += 1;
		}
		this.readCount.incrementAndGet();
	}

	
	protected void newOrder(RequestId requestId, int w_id,int d_id, int c_id, 
		int o_carrier_id, int o_myid, int i_id, int ol_myid, int ol_quantity, int retryTimes) throws IOException, InterruptedException{
		final String myid = "w_" + Integer.toString(w_id);
		TpccWarehouse warehouse = ((TpccWarehouse) dssInstance.Read(myid, "rw",requestId, retryTimes));
		final String dmyid = myid + "_" + Integer.toString(d_id);
		TpccDistrict district = ((TpccDistrict) dssInstance.Read(dmyid, "rw",requestId, retryTimes));
		TpccDistrict newDistrict=district.deepcopy();
		double D_TAX = district.D_TAX;
		int o_id = district.D_NEXT_O_ID;
		newDistrict.D_NEXT_O_ID = o_id + 1;
		newDistrict=((TpccDistrict) dssInstance.Write(dmyid, newDistrict, requestId,localId ));
		final String cmyid = myid + "_c_" + Integer.toString(c_id);	
		TpccCustomer customer = ((TpccCustomer) dssInstance.Read(cmyid, "rw",requestId, retryTimes));
		double C_DISCOUNT = customer.C_DISCOUNT;
		String C_LAST = customer.C_LAST;
		String C_CREDIT = customer.C_CREDIT;		
		// Create entries in ORDER and NEW-ORDER
		final String omyid = myid + "_o_"+ Integer.toString(o_myid);;
		TpccOrder order = new TpccOrder(omyid);
		order.O_C_ID = c_id;
		order.O_CARRIER_ID = Integer.toString(o_carrier_id); 															
		order.O_ALL_LOCAL = true;
		order=((TpccOrder) dssInstance.Write(omyid, order, requestId,localId ));
		int i = 1;
		while (i <= order.O_CARRIER_ID.length()) {
			String item_id = "i_" + Integer.toString(i_id);
			TpccItem item = ((TpccItem) dssInstance.Read(item_id, "rw",requestId, retryTimes));
			if (item == null) {
				System.out.println("Item is null >>>");
				System.exit(-1);
			}
			i += 1;
		}
	}

	protected void payment(RequestId requestId, float h_amount,int w_id, int c_id, int d_id, int retryTimes, boolean tpccProfile) throws IOException, InterruptedException {//I should make retry as a false value
		if(!tpccProfile) {
			final String myid = "w_" + Integer.toString(w_id);
			final String cmyid = myid + "_c_" + Integer.toString(c_id);
			TpccWarehouse warehouse = ((TpccWarehouse) dssInstance.Read(myid, "rw",requestId, retryTimes));
			TpccWarehouse newWarehouse=warehouse.deepcopy();
			newWarehouse.W_YTD += h_amount;
			newWarehouse=((TpccWarehouse) dssInstance.Write(myid, newWarehouse, requestId,localId ));
			final String dmyid = myid + "_" + Integer.toString(d_id);
			TpccDistrict district = ((TpccDistrict) dssInstance.Read(dmyid, "rw", requestId,retryTimes));
			TpccDistrict newDistrict=district.deepcopy();
			newDistrict.D_YTD += h_amount;
			newDistrict=((TpccDistrict) dssInstance.Write(dmyid, newDistrict, requestId,retryTimes ));
			TpccCustomer customer = ((TpccCustomer) dssInstance.Read(cmyid, "rw",requestId, retryTimes));
			TpccCustomer newCustomer=customer.deepcopy();
			newCustomer.C_BALANCE -= h_amount;
			newCustomer.C_YTD_PAYMENT += h_amount;
			newCustomer.C_PAYMENT_CNT += 1;
			newCustomer=((TpccCustomer) dssInstance.Write(cmyid, newCustomer, requestId,localId ));}
		else {
			final String myid = "w_" + Integer.toString(w_id);
			int customer_percentage=random.nextInt(100);
			String cmyid=null;
			if(customer_percentage<15) {
				//According to tpccProfile with 85% of probability customer is selected locally in tpccProfile
				int w_id_1=0;
				String warehouseId1=null;
				while(true) {
					w_id_1=random.nextInt(this.NUM_WAREHOUSES);
					warehouseId1="w_"+ Integer.toString(w_id_1);
					if(!getDssInstance().IfReplicate(warehouseId1)) {
						break;
					}
				}    
				final String myid_1 = "w_" + Integer.toString(w_id_1);
				cmyid = myid_1 + "_c_" + Integer.toString(c_id);
			}
			//According to tpccProfile with 85% of probability customer is selected locally in tpccProfile
			else {
				int w_id_1=0;
				String warehouseId1=null;
				while(true) {
					w_id_1=random.nextInt(this.NUM_WAREHOUSES);
					warehouseId1="w_"+ Integer.toString(w_id_1);
					if(getDssInstance().IfReplicate(warehouseId1)) {
						break;
					}
				}    
				final String myid_1 = "w_" + Integer.toString(w_id_1);
				cmyid = myid_1 + "_c_" + Integer.toString(c_id);
			}
			TpccWarehouse warehouse = ((TpccWarehouse) dssInstance.Read(myid, "rw",requestId, retryTimes));
			warehouse.W_YTD += h_amount;
			warehouse=((TpccWarehouse) dssInstance.Write(myid, warehouse, requestId,localId ));
			final String dmyid = myid + "_" + Integer.toString(d_id);
			TpccDistrict district = ((TpccDistrict) dssInstance.Read(dmyid, "rw", requestId,retryTimes));
			TpccDistrict newDistrict=district.deepcopy();
			newDistrict.D_YTD += h_amount;
			newDistrict=((TpccDistrict) dssInstance.Write(dmyid, newDistrict, requestId,retryTimes ));
			TpccCustomer customer = ((TpccCustomer) dssInstance.Read(cmyid, "rw",requestId, retryTimes));
			TpccCustomer newCustomer=customer.deepcopy();
			newCustomer.C_BALANCE -= h_amount;
			newCustomer.C_YTD_PAYMENT += h_amount;
			newCustomer.C_PAYMENT_CNT += 1;
			newCustomer=((TpccCustomer) dssInstance.Write(cmyid, newCustomer, requestId,localId ));
		}
	}


	public void setNode(Node node) {
		this.replica = node;
	}



	public byte[] serializeObject(AbstractObject object) throws IOException{
		ByteBuffer bb;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		if (object instanceof TpccWarehouse) {
			bb = ByteBuffer.allocate(2);//It has only version and type
			bb.putShort((short) 0);
			TpccWarehouse warehouse = (TpccWarehouse) object;
			bb.flip();
			out.write(bb.array());
		}
		else if (object instanceof TpccCustomer) {
			bb = ByteBuffer.allocate(2 + 8 + 8 + 4 + 4 );
			bb.putShort((short) 1);
			TpccCustomer customer = (TpccCustomer) object;
			bb.putDouble(customer.C_BALANCE);
			bb.putDouble(customer.C_YTD_PAYMENT);
			bb.putInt(customer.C_DELIVERY_CNT);
			bb.putInt(customer.C_PAYMENT_CNT);
			bb.flip();
			out.write(bb.array());

		} else if (object instanceof TpccDistrict) {
			bb = ByteBuffer.allocate(2 + 4 + 8);
			bb.putShort((short) 2);
			TpccDistrict district = (TpccDistrict) object;
			bb.putInt(district.D_NEXT_O_ID);
			bb.putDouble(district.D_YTD);
			bb.flip();
			out.write(bb.array());

		} else if (object instanceof TpccItem) {
			bb = ByteBuffer.allocate(2);
			bb.putShort((short) 3);
			TpccItem item = (TpccItem) object;
			bb.flip();
			out.write(bb.array());

		} else if (object instanceof TpccOrder) {
			TpccOrder order = (TpccOrder) object;
			String str = order.O_CARRIER_ID;
			byte[] strBytes = str.getBytes();
			bb = ByteBuffer.allocate(2 + 4 + 4 + strBytes.length + 1);
			bb.putShort((short) 4);
			bb.putInt(order.O_C_ID);
			bb.putInt(strBytes.length);
			bb.put(strBytes);
			bb.put(new byte[] { (byte) (order.O_ALL_LOCAL ? 1 : 0) });
			bb.flip();
			out.write(bb.array());

		} else if (object instanceof TpccOrderline) {
			TpccOrderline orderline = (TpccOrderline) object;
			String str = orderline.OL_DELIVERY_D;
			byte[] strBytes = str.getBytes();
			String str1 = orderline.OL_DIST_INFO;
			byte[] strBytes1 = str1.getBytes();
			bb = ByteBuffer.allocate(2 + 4 + 4 + 4 + 4 + 4
					+ strBytes.length + 4 + strBytes1.length);
			bb.putShort((short) 5);
			bb.putInt(orderline.OL_QUANTITY);
			bb.putInt(orderline.OL_I_ID);
			bb.putInt(orderline.OL_SUPPLY_W_ID);
			bb.putInt(orderline.OL_AMOUNT);
			bb.putInt(strBytes.length);
			bb.put(strBytes);
			bb.putInt(strBytes1.length);
			bb.put(strBytes1);
			bb.flip();
			out.write(bb.array());
		} else if (object instanceof TpccStock) {
			bb = ByteBuffer.allocate(2 /*+ 8*/);
			bb.putShort((short) 6);
			TpccStock stock = (TpccStock) object;
			bb.flip();
			out.write(bb.array());

		} else {
			System.out.println("Tpcc Object serialization: object not defined");
		}	
		return out.toByteArray();
	}	



	/**********************/	

	public AbstractObject deserializeObject(String Id,byte[] bytes){
		AbstractObject outObj=null;
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		String id=Id;
		short type = bb.getShort();
		switch (type) {
		case 0: {
			TpccWarehouse object = new TpccWarehouse(id);
			outObj=(AbstractObject)object;
			break;
		}
		case 1: {
			TpccCustomer object = new TpccCustomer(id);
			object.C_BALANCE = bb.getDouble();
			object.C_YTD_PAYMENT = bb.getDouble();
			object.C_DELIVERY_CNT = bb.getInt();
			object.C_PAYMENT_CNT = bb.getInt();
			outObj=(AbstractObject)object;
			break;
		}
		case 2: {
			TpccDistrict object = new TpccDistrict(id);
			object.D_NEXT_O_ID = bb.getInt();
			object.D_YTD = bb.getDouble();
			outObj=(AbstractObject)object;
			break;
		}
		case 3: {
			TpccItem object = new TpccItem(id);
			outObj=(AbstractObject)object;
			break;
		}
		case 4: {
			TpccOrder object = new TpccOrder(id);
			object.O_C_ID = bb.getInt();
			byte[] v = new byte[bb.getInt()];
			bb.get(v);
			String v_string =id;
			object.O_CARRIER_ID = v_string;
			object.O_ALL_LOCAL = (bb.get() != 0);
			outObj=(AbstractObject)object;
			break;
		}
		case 5: {
			TpccOrderline object = new TpccOrderline(id);

			object.OL_QUANTITY = bb.getInt();
			object.OL_I_ID = bb.getInt();
			object.OL_SUPPLY_W_ID = bb.getInt();
			object.OL_AMOUNT = bb.getInt();

			byte[] v = new byte[bb.getInt()];
			bb.get(v);
			String v_string=id;
			object.OL_DELIVERY_D = v_string;
			v = new byte[bb.getInt()];
			bb.get(v);
			object.OL_DIST_INFO = v_string;
			outObj=(AbstractObject)object;
			break;
		}
		case 6: {
			TpccStock object = new TpccStock(id);
			outObj=(AbstractObject)object;
			break;
		}
		default:
			System.out.println("Invalid Object Type");
			System.exit(-1);
		}
		return outObj;
	}

	@Override
	public Node getNode() {
		return this.replica;
	}


	public void onExecuteComplete(RequestId requestId,long timeOut) throws InterruptedException, IOException {
		getNode().getProcessintwopc().Prepare(requestId);
		TransactionContext ctx=dssInstance.getlocalTransactionContext(requestId);
		while(!ctx.getReady()) {
		}	
	}



	public void rollback1(RequestId requestId, int txType, int id1, int id2, int id3, int id4, int id5, int id6, int id7, int id8, float amount, int retryTimes, boolean tpccProfile){
		boolean r=true;
		while(r) {
			getNode().getProcessinRead().removeReadReq(requestId);
			dssInstance.removeTransactionContext(requestId);
			getNode().getProcessintwopc().removeTwopcSample(requestId);
			try {
				switch(txType) {
				case 3: 
					this.newOrder(requestId, id1, id2, id3, id4, id5, id6, id7, id8, retryTimes);
					r=false;
					break;

				case 4:
					this.delivery(requestId, id1, id2, id3, retryTimes);
					r=false;
					break;
				case 5:
					this.payment(requestId, amount, id1, id2, id3, retryTimes, tpccProfile);	
					r=false;
					break;

				default:
					System.out.println("Unknown transaction type!");
					System.exit(-1);
					break;
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(-1);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		/// 	System.out.println("RequestId "+requestId +" finished rollback successfully.");
	}



	public void on2PC(RequestId requestId, int txType, int id1, int id2, int id3, int id4, int id5, int id6, int id7, int id8, float amount, boolean tpccProfile) throws InterruptedException, IOException {
		while(dssInstance.getlocalTransactionContext(requestId).getRetry() ) {	
		abortedCount.incrementAndGet();
			int retryT=dssInstance.getlocalTransactionContext(requestId).incrementRetryTimes();
			rollback1(requestId,txType, id1, id2, id3, id4, id5, id6, id7, id8, amount,retryT, tpccProfile);
			onExecuteComplete(requestId,timeOut);
		}		
		dssInstance.getlocalTransactionContext(requestId).setReadyToRemove();
		//System.out.println("RequestId "+requestId+" committed");
	}


	@Override
	public byte[] serializeWriteSet(TransactionContext ctx) {
		try {	Map<String, AbstractObject> writeset = ctx.getWriteSet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteBuffer bb;
		for (Map.Entry<String, AbstractObject> entry : writeset.entrySet()) {
			String id = entry.getKey();
			byte[] idBytes = id.getBytes(Charset.forName("UTF-8"));
			ByteBuffer bb1 = ByteBuffer.allocate(idBytes.length + 4);
			bb1.putInt(idBytes.length);
			bb1.put(idBytes);
			bb1.flip();
			out.write(bb1.array());
			Object object = entry.getValue();
			if (object instanceof TpccWarehouse) {
				bb = ByteBuffer.allocate(2);
				bb.putShort((short) 0);
				TpccWarehouse warehouse = (TpccWarehouse) object;
				bb.flip();
				out.write(bb.array());
			}else if (object instanceof TpccCustomer) {
				bb = ByteBuffer.allocate(2 + 8 + 8 + 4 + 4);
				bb.putShort((short) 1);
				TpccCustomer customer = (TpccCustomer) object;
				bb.putDouble(customer.C_BALANCE);
				bb.putDouble(customer.C_YTD_PAYMENT);
				bb.putInt(customer.C_DELIVERY_CNT);
				bb.putInt(customer.C_PAYMENT_CNT);
				bb.flip();
				out.write(bb.array());

			}else if (object instanceof TpccDistrict) {
				bb = ByteBuffer.allocate(2 + 4 + 8);
				bb.putShort((short) 2);
				TpccDistrict district = (TpccDistrict) object;
				bb.putInt(district.D_NEXT_O_ID);
				bb.putDouble(district.D_YTD);
				bb.flip();
				out.write(bb.array());
			}else if (object instanceof TpccItem) {
				bb = ByteBuffer.allocate(2);
				bb.putShort((short) 3);
				TpccItem item = (TpccItem) object;
				bb.flip();
				out.write(bb.array());
			} else if (object instanceof TpccOrder) {
				TpccOrder order = (TpccOrder) object;
				String str = order.O_CARRIER_ID;
				byte[] strBytes = str.getBytes();
				bb = ByteBuffer.allocate(2 + 4 + 4 + strBytes.length + 1);
				bb.putShort((short) 4);
				bb.putInt(order.O_C_ID);
				bb.putInt(strBytes.length);
				bb.put(strBytes);
				bb.put(new byte[] { (byte) (order.O_ALL_LOCAL ? 1 : 0) });
				bb.flip();
				out.write(bb.array());
			}  else if (object instanceof TpccOrderline) {
				TpccOrderline orderline = (TpccOrderline) object;
				String str = orderline.OL_DELIVERY_D;
				byte[] strBytes = str.getBytes();
				String str1 = orderline.OL_DIST_INFO;
				byte[] strBytes1 = str1.getBytes();
				bb = ByteBuffer.allocate(2 + 4 + 4 + 4 + 4 + 4
						+ strBytes.length + 4 + strBytes1.length );
				bb.putShort((short) 5);
				bb.putInt(orderline.OL_QUANTITY);
				bb.putInt(orderline.OL_I_ID);
				bb.putInt(orderline.OL_SUPPLY_W_ID);
				bb.putInt(orderline.OL_AMOUNT);
				bb.putInt(strBytes.length);
				bb.put(strBytes);
				bb.putInt(strBytes1.length);
				bb.put(strBytes1);
				bb.flip();
				out.write(bb.array());
			}else if (object instanceof TpccStock) {
				bb = ByteBuffer.allocate(2);
				bb.putShort((short) 6);
				TpccStock stock = (TpccStock) object;
				bb.flip();
				out.write(bb.array());
			} else {
				System.out
				.println("Tpcc Object serialization: object not defined");
			}
		}

		byte[] output=out.toByteArray();
		ctx.setWriteSetByteSize(output.length);
		return output;
		}
		catch(IOException e){
			e.printStackTrace();
			System.exit(-1);
			return null;
		}
	}

	@Override
	public Map<String, AbstractObject> deserializeReadSet(int rsize, byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		Map<String,AbstractObject>readset=new HashMap<String, AbstractObject>();
		for (int i = 0; i < rsize; i++) {
			byte[] value = new byte[bb.getInt()];
			bb.get(value);
			String id = new String(value, Charset.forName("UTF-8"));
			short type = bb.getShort();
			switch (type) {
			case 0: {
				TpccWarehouse object = new TpccWarehouse(id);
				readset.put(id, object);
				break;
			}
			case 1: {
				TpccCustomer object = new TpccCustomer(id);
				object.C_BALANCE = bb.getDouble();
				object.C_YTD_PAYMENT = bb.getDouble();
				object.C_DELIVERY_CNT = bb.getInt();
				object.C_PAYMENT_CNT = bb.getInt();
				readset.put(id, object);
				break;
			}
			case 2: {
				TpccDistrict object = new TpccDistrict(id);
				object.D_NEXT_O_ID = bb.getInt();
				object.D_YTD = bb.getDouble();
				readset.put(id, object);
				break;
			}
			case 3: {
				TpccItem object = new TpccItem(id);
				readset.put(id, object);
				break;
			}
			case 4: {
				TpccOrder object = new TpccOrder(id);
				object.O_C_ID = bb.getInt();
				byte[] v = new byte[bb.getInt()];
				bb.get(v);
				String v_string = new String(value, Charset.forName("UTF-8"));
				object.O_CARRIER_ID = v_string;
				object.O_ALL_LOCAL = (bb.get() != 0);
				readset.put(id, object);
				break;
			}
			case 5: {
				TpccOrderline object = new TpccOrderline(id);

				object.OL_QUANTITY = bb.getInt();
				object.OL_I_ID = bb.getInt();
				object.OL_SUPPLY_W_ID = bb.getInt();
				object.OL_AMOUNT = bb.getInt();

				byte[] v = new byte[bb.getInt()];
				bb.get(v);
				String v_string = new String(value, Charset.forName("UTF-8"));
				object.OL_DELIVERY_D = v_string;
				v = new byte[bb.getInt()];
				bb.get(v);
				v_string = new String(value, Charset.forName("UTF-8"));
				object.OL_DIST_INFO = v_string;
				readset.put(id, object);
				break;
			}
			case 6: {
				TpccStock object = new TpccStock(id);
				readset.put(id, object);
				break;
			}
			default:
				System.out.println("Invalid Object Type");
			}

		}
		return readset;
	}

	@Override
	public byte[] serializeReadSet(TransactionContext ctx)  {
		try {	Map<String, ReadSetEntry> readset = ctx.getReadSet();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ByteBuffer bb;
		for (Map.Entry<String, ReadSetEntry> entry : readset.entrySet()) {
			String id = entry.getKey();
			byte[] idBytes = id.getBytes(Charset.forName("UTF-8"));
			ByteBuffer bb1 = ByteBuffer.allocate(idBytes.length + 4);
			bb1.putInt(idBytes.length);
			bb1.put(idBytes);
			bb1.flip();
			out.write(bb1.array());
			Object object = entry.getValue();

			if (object instanceof TpccWarehouse) {
				bb = ByteBuffer.allocate(2);
				bb.putShort((short) 0);
				TpccWarehouse warehouse = (TpccWarehouse) object;
				bb.flip();
				out.write(bb.array());

			}else if (object instanceof TpccCustomer) {
				bb = ByteBuffer.allocate(2 + 8 + 8 + 4 + 4);
				bb.putShort((short) 1);
				TpccCustomer customer = (TpccCustomer) object;
				bb.putDouble(customer.C_BALANCE);
				bb.putDouble(customer.C_YTD_PAYMENT);
				bb.putInt(customer.C_DELIVERY_CNT);
				bb.putInt(customer.C_PAYMENT_CNT);
				bb.flip();
				out.write(bb.array());

			}else if (object instanceof TpccDistrict) {
				bb = ByteBuffer.allocate(2 + 4 + 8);
				bb.putShort((short) 2);
				TpccDistrict district = (TpccDistrict) object;
				bb.putInt(district.D_NEXT_O_ID);
				bb.putDouble(district.D_YTD);
				bb.flip();
				out.write(bb.array());
			}else if (object instanceof TpccItem) {
				bb = ByteBuffer.allocate(2);
				bb.putShort((short) 3);
				TpccItem item = (TpccItem) object;
				bb.flip();
				out.write(bb.array());

			} else if (object instanceof TpccOrder) {
				TpccOrder order = (TpccOrder) object;
				String str = order.O_CARRIER_ID;
				byte[] strBytes = str.getBytes();
				bb = ByteBuffer.allocate(2 + 4 + 4 + strBytes.length + 1 );
				bb.putShort((short) 4);
				bb.putInt(order.O_C_ID);
				bb.putInt(strBytes.length);
				bb.put(strBytes);
				bb.put(new byte[] { (byte) (order.O_ALL_LOCAL ? 1 : 0) });
				bb.flip();
				out.write(bb.array());

			}  else if (object instanceof TpccOrderline) {

				TpccOrderline orderline = (TpccOrderline) object;

				String str = orderline.OL_DELIVERY_D;
				byte[] strBytes = str.getBytes();

				String str1 = orderline.OL_DIST_INFO;
				byte[] strBytes1 = str1.getBytes();

				bb = ByteBuffer.allocate(2 + 4 + 4 + 4 + 4 + 4+ strBytes.length + 4 + strBytes1.length);
				bb.putShort((short) 5);
				bb.putInt(orderline.OL_QUANTITY);
				bb.putInt(orderline.OL_I_ID);
				bb.putInt(orderline.OL_SUPPLY_W_ID);
				bb.putInt(orderline.OL_AMOUNT);
				bb.putInt(strBytes.length);
				bb.put(strBytes);
				bb.putInt(strBytes1.length);
				bb.put(strBytes1);
				bb.flip();
				out.write(bb.array());
			}else if (object instanceof TpccStock) {
				bb = ByteBuffer.allocate(2);
				bb.putShort((short) 6);
				TpccStock stock = (TpccStock) object;
				bb.flip();
				out.write(bb.array());

			} else {
				System.out
				.println("Tpcc Object serialization: object not defined");
			}
		}

		byte[] output=out.toByteArray();
		ctx.setReadSEtByteSIze(output.length);
		return output;
		}
		catch(IOException e){
			e.printStackTrace();
			System.exit(-1);
			return null;
		}

	}



	@Override
	public Map<String, AbstractObject> deserializeWriteSet(int wsize, byte[] bytes) {
		ByteBuffer bb = ByteBuffer.wrap(bytes);
		Map<String,AbstractObject>writeset=new HashMap<String, AbstractObject>();
		for (int i = 0; i < wsize; i++) {
			byte[] value = new byte[bb.getInt()];
			bb.get(value);
			String id = new String(value, Charset.forName("UTF-8"));
			short type = bb.getShort();
			switch (type) {
			case 0: {
				TpccWarehouse object = new TpccWarehouse(id);
				writeset.put(id, object);
				break;
			}
			case 1: {
				TpccCustomer object = new TpccCustomer(id);
				object.C_BALANCE = bb.getDouble();
				object.C_YTD_PAYMENT = bb.getDouble();
				object.C_DELIVERY_CNT = bb.getInt();
				object.C_PAYMENT_CNT = bb.getInt();
				writeset.put(id, object);
				break;
			}
			case 2: {
				TpccDistrict object = new TpccDistrict(id);
				object.D_NEXT_O_ID = bb.getInt();
				object.D_YTD = bb.getDouble();
				writeset.put(id, object);
				break;
			}
			case 3: {
				TpccItem object = new TpccItem(id);
				writeset.put(id, object);
				break;
			}
			case 4: {
				TpccOrder object = new TpccOrder(id);
				object.O_C_ID = bb.getInt();
				byte[] v = new byte[bb.getInt()];
				bb.get(v);
				String v_string = new String(value, Charset.forName("UTF-8"));
				object.O_CARRIER_ID = v_string;
				object.O_ALL_LOCAL = (bb.get() != 0);
				writeset.put(id, object);
				break;
			}
			case 5: {
				TpccOrderline object = new TpccOrderline(id);

				object.OL_QUANTITY = bb.getInt();
				object.OL_I_ID = bb.getInt();
				object.OL_SUPPLY_W_ID = bb.getInt();
				object.OL_AMOUNT = bb.getInt();
				byte[] v = new byte[bb.getInt()];
				bb.get(v);
				String v_string = new String(value, Charset.forName("UTF-8"));
				object.OL_DELIVERY_D = v_string;
				v = new byte[bb.getInt()];
				bb.get(v);
				v_string = new String(value, Charset.forName("UTF-8"));
				object.OL_DIST_INFO = v_string;
				writeset.put(id, object);
				break;
			}
			case 6: {
				TpccStock object = new TpccStock(id);
				writeset.put(id, object);
				break;
			}
			default:
				System.out.println("Invalid Object Type in deserialize WriteSet");
				System.out.println("case is equal to "+type);
				System.exit(-1);
			}
		}
		return writeset;
	}

	@Override
	public void IncreaseCommitCount() {
		committedCount.incrementAndGet();		
	}

	public Wcc getDssInstance() {
		return this.dssInstance;
	}

	public int getLocalityPercentage() {
		return this.localityPercentage;
	}


	public boolean getTpccProfile() {
		return this.tpccProfile;
	}


   public Node getReplica() {
	   return this.replica;
   }


@Override
public void addToCollectedNum(int collectedNum) {
	// TODO Auto-generated method stub
	
}

}
