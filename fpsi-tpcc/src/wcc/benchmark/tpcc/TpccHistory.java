package wcc.benchmark.tpcc;

import java.util.Random;

import wop.transaction.AbstractObject;

public class TpccHistory extends AbstractObject implements java.io.Serializable {	
	private static final long serialVersionUID = 1L;
	public int H_C_ID;
	public int H_C_D_ID;
	public int H_C_W_ID;
	public int H_D_ID;
	public int H_W_ID;
	public String H_DATE;
	public Double H_AMOUNT;
	public String H_DATA;

	private Random random = new Random();

	private String id;

	public TpccHistory() {
		super(Tpcc.numNodes);
	}

	public TpccHistory(String id, int c_id, int d_id) {
		super(Tpcc.numNodes);
		this.id = id;
		this.H_W_ID = random.nextInt(100);
		this.H_D_ID = d_id;
		this.H_C_ID = c_id;
		this.H_DATE = Integer.toString(random.nextInt(100));
		this.H_AMOUNT = 10.0;
		this.H_DATA = Integer.toString(random.nextInt(100));
	}

	public String getId() {
		return id;
	}

}