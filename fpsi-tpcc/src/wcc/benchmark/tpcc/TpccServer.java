package wcc.benchmark.tpcc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import wcc.Wcc;
import wcc.Store;
import wop.TwoPhaseCommit.replica.Node;
import wcc.common.Configuration;





public class TpccServer {



	public static void main(String[] args) throws IOException,
	InterruptedException, ExecutionException {
		if (args.length < 15 || args.length > 15) {
			usage();
			System.exit(1);
		}

		int localId = Integer.parseInt(args[0]);
		int warehouseCount = Integer.parseInt(args[1]); 
		int itemCount = Integer.parseInt(args[2]);
		int readPercentage = Integer.parseInt(args[3]);
		int clientCount = Integer.parseInt(args[4]);
		int numRequests = Integer.parseInt(args[5]);
		long Timeout=Long.parseLong(args[6]);
		int replicationDegree=Integer.parseInt(args[7]);
		int localityPercentage=Integer.parseInt(args[8]);
		boolean tpccProfile = Integer.parseInt(args[9]) == 0 ? false : true;

		int orderStatusPercentage=Integer.parseInt(args[10]);
		int stockLevelPercentage=Integer.parseInt(args[11]);
		int neworderPercentage=Integer.parseInt(args[12]);
		int paymentPercentage=Integer.parseInt(args[13]);
		int deliveryPercentage=Integer.parseInt(args[14]);



		Configuration process = new Configuration();
		Store store=new Store(warehouseCount ,localId,process.getN(),replicationDegree);
		Wcc dssInstance=new Wcc(store,process.getN(),localId);
		Tpcc tpcc = new Tpcc(process.getN());
		tpcc.TpccInit(store, dssInstance, numRequests, localId, process.getN(), warehouseCount, itemCount,localityPercentage, Timeout,tpccProfile);		
		dssInstance.init(tpcc);
		Node node = new Node(process,tpcc,localId,dssInstance,process.getN(),Timeout);
		node.start();
		tpcc.setNode(node);
		tpcc.initRequests();
		System.out.println("I am starting debugging FPSI with TPCC ");
		TpccMultiClient client = new TpccMultiClient(clientCount, numRequests,readPercentage, tpccProfile, tpcc);
		client.run(orderStatusPercentage, stockLevelPercentage, neworderPercentage,paymentPercentage, deliveryPercentage);
		System.in.read();
	}

	private static void usage() {
		System.out
		.println("Invalid arguments. Usage:\n"
				+ "   java lsr.paxos.Replica <localId> "
				+ "<Number Of Warehouses - 20> "
				+ "<Number Of Items - 5000> "
				+  "<readPercentage> <clientCount> <numRequests> <Timeout> <replicationDegree> <localityPercentage> <tpccProfile>");

	}
}