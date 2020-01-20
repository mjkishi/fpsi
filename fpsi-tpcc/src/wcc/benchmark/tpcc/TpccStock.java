package wcc.benchmark.tpcc;

import java.util.Random;
import wop.transaction.AbstractObject;

public class TpccStock extends AbstractObject implements java.io.Serializable {	
	private static final long serialVersionUID = 1L;
	public int S_QUANTITY;
	public String S_DIST_01;
	public String S_DIST_02;
	public String S_DIST_03;
	public String S_DIST_04;
	public String S_DIST_05;
	public String S_DIST_06;
	public String S_DIST_07;
	public String S_DIST_08;
	public String S_DIST_09;
	public String S_DIST_10;
	public int S_YTD;
	public int S_ORDER_CNT;
	public int S_REMOTE_CNT;
	public String S_DATA;
	private Random random = new Random();
	
	private String id;
	
	private String genData(){
		if (random.nextInt(100) < 10) {
			String data = Integer.toString(random.nextInt(100));
			return data+"_ORIGINAL";
		} else {
			return Integer.toString(random.nextInt(100));
		}
	}
	
	public TpccStock() {
		super(Tpcc.numNodes);
	}

	public TpccStock(String id) {
		super(Tpcc.numNodes);
		this.id = id;
		this.S_QUANTITY = 10 + random.nextInt(91);
		this.S_DIST_01 = Integer.toString(random.nextInt(100));
		this.S_DIST_02 = Integer.toString(random.nextInt(100));
		this.S_DIST_03 = Integer.toString(random.nextInt(100));
		this.S_DIST_04 = Integer.toString(random.nextInt(100));
		this.S_DIST_05 = Integer.toString(random.nextInt(100));
		this.S_DIST_06 = Integer.toString(random.nextInt(100));
		this.S_DIST_07 = Integer.toString(random.nextInt(100));
		this.S_DIST_08 = Integer.toString(random.nextInt(100));
		this.S_DIST_09 = Integer.toString(random.nextInt(100));
		this.S_DIST_10 = Integer.toString(random.nextInt(100));
		this.S_YTD = 0;
		this.S_ORDER_CNT = 0;
		this.S_REMOTE_CNT = 0;
		this.S_DATA = genData();	
	}

	public String getId() {
		return id;
	}
	
}