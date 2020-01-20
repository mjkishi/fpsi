package wcc;


import wcc.common.RequestId;

public class NodeStatus {

	private int[] nodeStatus;
	private int numNodes;


	public NodeStatus(int numNodes) {
		nodeStatus=new int[numNodes];
		for(int i=0; i<numNodes; i++){
			nodeStatus[i]=0;
		}
		this.numNodes=numNodes;
	}

	public int[] getNodeStatus() {
		synchronized(this) {
			int[] TxVC=new int [numNodes];
			for(int i=0;i<numNodes;i++) {
				TxVC[i]=nodeStatus[i];
			}
			return TxVC;
		}
	}


	public int[] UpdateNodeStatus(RequestId reqId,int sid, int nodeId, String line) throws InterruptedException {			
		synchronized(this) {
			int[] commitVC=new int[numNodes];
			nodeStatus[nodeId]=sid;
			/*for(int i=0;i<numNodes;i++) {
		 						System.out.println("										NodeStatus after update by"+reqId +" at position "+i+" is "+nodeStatus[i]+line);
				commitVC[i]=nodeStatus[i];
			}*/
			return commitVC;
		}
	}
}
