package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import wcc.common.RequestId;



public final class PrepareAns extends Message {
	private static final long serialVersionUID = 1L;
	private int success;
	private int sizeOfCollectedRW;
	private byte[] collectedRW;

	public PrepareAns(RequestId requestId, int success, int sizeOfCollectedRW, byte[] collectedRW ) {
		super(requestId);
		this.success=success;
		this.sizeOfCollectedRW=sizeOfCollectedRW;
		this.collectedRW=collectedRW;
	}


	public RequestId getRequestId(){
		return super.getRequestId();
	}

	public int getSizeOfCollectedRW() {
		return this.sizeOfCollectedRW;
	}


	public byte[] getCollectedRW() {
		return collectedRW;
	}


	public int getSuccess() {
		return this.success;
	}


	public PrepareAns(DataInputStream input) throws IOException { 	
		super(input);
		this.success=input.readInt();
		this.sizeOfCollectedRW=input.readInt();
		this.collectedRW=new byte[sizeOfCollectedRW];
		input.readFully(collectedRW);
	}

	public PrepareAns(Prepare msg, int success, int sizeOfCollectedRW, byte[] collectedRW){
		super(msg.getRequestId());
		this.success=success;
		this.sizeOfCollectedRW=sizeOfCollectedRW;
		this.collectedRW=collectedRW;
	}

	public MessageType getType() {
		return MessageType.PrepareAns;
	}

	public int byteSize() {
		return  super.byteSize()+4+4+collectedRW.length;
	}




	public String toString() {
		return "PrepareAns(" + super.toString() +")";
	}


	protected void write(ByteBuffer bb) {
		bb.putInt(success);
		bb.putInt(sizeOfCollectedRW);
		bb.put(collectedRW);
	}

}