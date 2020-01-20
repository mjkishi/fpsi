package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import wcc.common.RequestId;


public final class Read extends Message {
	private static final long serialVersionUID = 1L;
	private int sizeOfId;
	private String ObjectId;
	private byte[] VC;
	private byte[] hasRead;
	private int numNodes;
	private int isUpdate;
	private int retryTimes;


	public Read(RequestId requestId, int sizeOfId,String ObjectId, int numNodes, byte[] VC, byte[] hasRead, int isUpdate, int retryTimes) {
		super(requestId);
		this.sizeOfId=sizeOfId;
		this.ObjectId=ObjectId;
		this.VC=VC;
		this.hasRead=hasRead;
		this.numNodes=numNodes;
		this.isUpdate=isUpdate;
		this.retryTimes=retryTimes;

	}


	public RequestId getRequestId(){
		return super.getRequestId();
	}


	public int getSizeId(){
		return sizeOfId;
	}


	public String getObjectId(){
		return ObjectId;
	}

	public byte[] getVC(){
		return this.VC;
	}

	public int getRetryTimes() {
		return this.retryTimes;
	}

	public byte[] gethasRead(){
		return this.hasRead;
	}

	public int getIsUpdate() {
		return isUpdate;
	}

	public Read(DataInputStream input) throws IOException { 	
		super(input);
		sizeOfId=input.readInt();
		byte[] ObjIdbyte=new byte[sizeOfId];
		input.readFully(ObjIdbyte);
		this.ObjectId=new String(ObjIdbyte,"UTF-8") ; 
		numNodes=input.readInt();
		hasRead=new byte[numNodes*4];
		input.readFully(hasRead);
		VC=new byte[numNodes*4];
		input.readFully(VC);
		isUpdate=input.readInt();
		retryTimes=input.readInt();
	}


	public MessageType getType() {
		return MessageType.Read;
	}

	public int byteSize() {
		return  super.byteSize() +4+ObjectId.length()+4+(numNodes*4)+4+(numNodes*4)+4;
	}

	public String toString() {
		return "Read(" + super.toString() +")";
	}


	protected void write(ByteBuffer bb) {
		bb.putInt(sizeOfId);
		bb.put(ObjectId.getBytes());
		bb.putInt(numNodes);
		bb.put(hasRead);
		bb.put(VC);
		bb.putInt(isUpdate);
		bb.putInt(retryTimes);

	}

}