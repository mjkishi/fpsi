package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import wcc.common.RequestId;



public final class Propagate extends Message {
	private static final long serialVersionUID = 1L;
	private byte[] StartVC;
	private int numNodes;
	private int seqNo;


	public Propagate(RequestId requestId,byte[] StartVC, int seqNo,int numNodes) {
		super(requestId);
		this.StartVC=StartVC;
		this.numNodes=numNodes;
		this.seqNo=seqNo;
	}


	public RequestId getRequestId(){
		return super.getRequestId();
	}

	public byte[] getStartVC() {
		return this.StartVC;
	}


	public int getSeqNo() {
		return this.seqNo;
	}


	public Propagate(DataInputStream input) throws IOException { 	
		super(input);
		this.seqNo=input.readInt();
		numNodes=input.readInt();
		this.StartVC=new byte[numNodes*4];
		input.readFully(StartVC);
	}



	public MessageType getType() {
		return MessageType.Propagate;
	}

	public int byteSize() {
		return  super.byteSize()+4+4+(numNodes*4);
	}




	public String toString() {
		return "Propagate(" + super.toString() +")";
	}


	protected void write(ByteBuffer bb) {
		bb.putInt(seqNo);
		bb.putInt(numNodes);
		bb.put(StartVC);
	}

}