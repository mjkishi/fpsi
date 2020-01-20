package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import wcc.common.RequestId;




public final class Prepare extends Message {
	private static final long serialVersionUID = 1L;
	private int probablesn;
	private int wsize;
	private byte[]  writeSet;
	private byte[] StartVC;
	private final int objIdbytesize=16;
	private int numNodes;



	public Prepare(RequestId requestId,  int probablesn, int wsize,byte[] writeSet, byte[] StartVC, int numNodes ) {	
		super(requestId);
		this.probablesn=probablesn;
		this.wsize=wsize;
		assert writeSet != null;
		this.writeSet=writeSet;
		this.StartVC=StartVC;
		this.numNodes=numNodes;
	}

	public int getProbableSn() {
		return this.probablesn;
	}


	public RequestId getRequestId(){
		return super.getRequestId();
	}


	public int getwsize(){
		return wsize;
	}


	public byte[] getwriteSet(){
		return writeSet;
	}



	public byte[] getStartVC() {
		return this.StartVC;
	}


	public Prepare(DataInputStream input) throws IOException { 	    
		super(input); 	
		probablesn=input.readInt();   	
		wsize=input.readInt(); 	
		this.writeSet=new byte[wsize*objIdbytesize];
		input.readFully(writeSet); 	
		numNodes=input.readInt();	 
		this.StartVC=new byte[numNodes*4];
		input.readFully(StartVC);
	}


	public MessageType getType() {
		return MessageType.Prepare;
	}

	public int byteSize() { 	
		return  super.byteSize() +4+(wsize*objIdbytesize)+4+4+(numNodes*4);
	}

	public String toString() {
		return "Prepare(" + super.toString() +")";
	}


	protected void write(ByteBuffer bb) {
		bb.putInt(probablesn);
		bb.putInt(wsize);
		bb.put(writeSet);
		bb.putInt(numNodes);
		bb.put(StartVC);

	}

}