package wop.TwoPhaseCommit.replica;

import java.io.IOException;

import wcc.Wcc;
import wcc.common.Configuration;
import wcc.common.ProcessDescriptor;
import wop.TwoPhaseCommit.ProcessinRead;
import wop.TwoPhaseCommit.Processintwopc;
import wop.service.STMService;




public class Node {
	Processintwopc processintwopc;
	ProcessinRead processinread;


	public Node(Configuration config,STMService service, int localId, Wcc wccInstance, int numNodes, long Timeout) throws IOException {
		ProcessDescriptor.initialize(config, localId);
		processintwopc=new Processintwopc(wccInstance,localId);
		processinread=new ProcessinRead(wccInstance,localId,numNodes);		
	}

	public void start() throws IOException {
		processintwopc.startTwopcforProcess();
		processinread.startProcessforRead(/*processintwopc.getNetwork()*/);
	}


	public Processintwopc getProcessintwopc(){
		return processintwopc;
	}

	public ProcessinRead getProcessinRead(){
		return processinread;
	}


	public Wcc getWcc(){
		return  processintwopc.getWccInstance();
	}

}