package wcc.benchmark.tpcc;

import java.util.Random;

import wop.transaction.AbstractObject;

public class TpccOrderline extends AbstractObject implements java.io.Serializable {	
	private static final long serialVersionUID = 1L;
	public int OL_I_ID; 
	public int OL_SUPPLY_W_ID; 
	public String OL_DELIVERY_D; 
	public int OL_QUANTITY; 
	public int OL_AMOUNT; 
	public String OL_DIST_INFO; 
	
	private Random random = new Random();
	private String id;

	private int genAmount(int a){
		if (a < 2101) return 0; 
		else { 
			return (1 + random.nextInt(999999));
		}
		
	}
	
	public TpccOrderline() {
		super(Tpcc.numNodes);
	}
	
	public TpccOrderline(String id) {
		super(Tpcc.numNodes);

		this.id = id;
		
		this.OL_I_ID = 1 + random.nextInt(100000);
		this.OL_SUPPLY_W_ID = random.nextInt(1000);
		this.OL_DELIVERY_D = Integer.toString(random.nextInt(100));
		this.OL_QUANTITY = 5;
		this.OL_AMOUNT = genAmount(random.nextInt(3000));
		this.OL_DIST_INFO = Integer.toString(random.nextInt(100));
	}

	public String getId() {
		return id;
	}

}