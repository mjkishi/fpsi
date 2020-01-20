package wcc.benchmark.tpcc;

import java.util.Random;

import wop.transaction.AbstractObject;

public class TpccOrder extends AbstractObject implements java.io.Serializable {	
	private static final long serialVersionUID = 1L;
	public int O_C_ID;
	public String O_ENTRY_D;
	public String O_CARRIER_ID;
	public int O_OL_CNT;
	public Boolean O_ALL_LOCAL;
	
	private Random random = new Random();
	
	private String id;

	public TpccOrder() {
		super(Tpcc.numNodes);
	}
	
	public TpccOrder(String id) {
		super(Tpcc.numNodes);
		this.id = id;
		this.O_C_ID = random.nextInt(100);
		this.O_ENTRY_D = Integer.toString(random.nextInt(100));
		this.O_CARRIER_ID = Integer.toString(random.nextInt(15));
		this.O_OL_CNT = 5 + random.nextInt(11);
		this.O_ALL_LOCAL = true;
	}

	public String getId() {
		return id;
	}

}