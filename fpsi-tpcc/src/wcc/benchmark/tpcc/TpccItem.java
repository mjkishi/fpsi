package wcc.benchmark.tpcc;


import java.util.Random;

import wop.transaction.AbstractObject;


public class TpccItem extends AbstractObject implements java.io.Serializable {	
	private static final long serialVersionUID = 1L;
	public String I_IM_ID;
	public String I_NAME;
	public float I_PRICE;
	public String I_DATA;
	private Random random = new Random();
	private String id;

	public TpccItem() {
		super(Tpcc.numNodes);
	}

	public TpccItem(String id) {
		super(Tpcc.numNodes);
		this.id = id;
		this.I_IM_ID = Integer.toString(random.nextInt(100));
		this.I_NAME = Integer.toString(random.nextInt(100));
		this.I_PRICE = random.nextFloat();
		this.I_DATA = Integer.toString(random.nextInt(100));
	}


	public String getId() {
		return id;
	}
}