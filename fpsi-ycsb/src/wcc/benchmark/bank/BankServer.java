package wcc.benchmark.bank;

import java.io.IOException;
import java.util.concurrent.ExecutionException;



import wcc.Store;
import wcc.Wcc;
import wcc.common.Configuration;
import wop.TwoPhaseCommit.replica.Node;


public class BankServer {

	public static void main(String[] args) throws IOException,
	InterruptedException, ExecutionException {
		if (args.length < 8 || args.length > 8) {
			usage();
			System.exit(1);
		}

		int localId = Integer.parseInt(args[0]);
		int objectsCount = Integer.parseInt(args[1]);
		int readPercentage = Integer.parseInt(args[2]);
		int clientCount = Integer.parseInt(args[3]);
		int requests = Integer.parseInt(args[4]);
		long Timeout=Long.parseLong(args[5]);
		int replicationDegree=Integer.parseInt(args[6]);
		int localityPercentage=Integer.parseInt(args[7]);		
		Configuration process = new Configuration();
		System.out.println(" Hey Masoomeh LocalId is "+localId);
		Bank bank = new Bank(process.getN());
		Store store=new Store(objectsCount,localId,process.getN(),replicationDegree);
		Wcc wccInstance= new Wcc(store, process.getN(),localId);
		bank.init(requests*clientCount,objectsCount, store, wccInstance,process.getN() ,localId, localityPercentage);  
		store.setNodeStatus(wccInstance.getNodeStatus());
		Node node = new Node(process,bank,localId,wccInstance,process.getN(),Timeout,objectsCount);
		node.start();// 
		bank.setNode(node);
		System.out.println("------FPSI on YCSB------");
		wccInstance.init(bank);
		BankMultiClient client = new BankMultiClient(clientCount, requests,
				objectsCount, readPercentage, bank);
		bank.init();
		client.run();
		System.in.read();
	}

	private static void usage() {
		System.out.println("Invalid arguments. Usage:\n"
				+ "  <NodeID> "
				+ "<Number Of Object> <read%> <clients> <requests>"
				+ "<% TimeOut for lock");
	}
}