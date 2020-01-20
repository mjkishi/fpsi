package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import wcc.common.RequestId;


public final class Commit extends Message {
	private static final long serialVersionUID = 1L;
	private int wsize;
	private byte[] writeSet;
	private final byte[] commitVTS;
	private int sn;
	private final int objIdbytesize=16;//tHIS 16 IS DEFINED
	private int numNodes;
	private int sizeOfCollectedRW;
	private byte[] collectedRW;

	public Commit(RequestId requestId, int wsize,byte[] writeSet,byte[] commitVTS, int sn , int numNodes,int sizeOfCollectedRW, byte[] collectedRW) {
		super(requestId);
		this.wsize=wsize;
		this.writeSet=writeSet;
		assert commitVTS != null;
		this.commitVTS =commitVTS;
		this.sn=sn;
		this.numNodes=numNodes;
		this.sizeOfCollectedRW=sizeOfCollectedRW;
		this.collectedRW=collectedRW;

	}

	public RequestId getRequestId(){
		return super.getRequestId();
	}

	public byte[] getwriteSet(){
		return writeSet;
	}

	public byte[] getCommitVTS(){
		return this.commitVTS;
	}


	public int getSeqNo(){
		return sn;
	}

	public int getwsize(){
		return wsize;
	}


	public int getSizeOfCollectedRW() {
		return sizeOfCollectedRW;
	}



	public byte[] getCollectedRW() {
		return this.collectedRW;
	}

	public Commit(DataInputStream input) throws IOException {
		super(input);
		this.wsize=input.readInt();
		this.writeSet=new byte[wsize*objIdbytesize];
		input.readFully(writeSet);
		this.numNodes=input.readInt();
		commitVTS = new byte[numNodes*4];
		input.readFully(commitVTS);
		sn=input.readInt();
		sizeOfCollectedRW=input.readInt();
		collectedRW=new byte[sizeOfCollectedRW];
		input.readFully(collectedRW);

	}

	public Commit(PrepareAns msg, int wsize,byte[] writeSet,int numNodes,byte[] commitVTS, int sn, int sizeOfCollectedRW, byte[] collectedRW){
		super(msg.getRequestId());
		this.wsize=wsize;
		this.writeSet=writeSet;
		this.numNodes=numNodes;
		this.commitVTS=commitVTS;
		this.sn=sn;
		this.sizeOfCollectedRW=sizeOfCollectedRW;
		this.collectedRW=collectedRW;

	}

	public MessageType getType() {
		return MessageType.Commit;
	}

	public int byteSize() {
		return  super.byteSize() +4+wsize*objIdbytesize+4 + (numNodes*4)+4+4+collectedRW.length;
	}



	public String toString() {
		return "Commit(" + super.toString() +")";
	}


	protected void write(ByteBuffer bb) {
		bb.putInt(wsize);
		bb.put(writeSet);
		bb.putInt(numNodes);
		bb.put(commitVTS);
		bb.putInt(sn);
		bb.putInt(sizeOfCollectedRW);
		bb.put(collectedRW);
	}	  
}