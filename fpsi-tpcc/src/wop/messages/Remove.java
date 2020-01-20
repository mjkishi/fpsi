package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import wcc.common.RequestId;

public final class Remove extends Message {
	private static final long serialVersionUID = 1L;
	int siteId;
	int objSeqNo;
	private int sizeOfId;
	private String ObjectId;


	public Remove(RequestId requestId,int siteId, int objSeqNo, int sizeOfId, String objId) {
		super(requestId);
		this.siteId=siteId;
		this.objSeqNo=objSeqNo;
		this.sizeOfId=sizeOfId;
		this.ObjectId=objId;
	}


	public RequestId getRequestId(){
		return super.getRequestId();
	}

	public int getSiteId(){
		return this.siteId;
	}

	public int getObjSeqNo() {
		return this.objSeqNo;
	}

	public int getSizeId(){
		return sizeOfId;
	}


	public String getObjectId(){
		return ObjectId;
	}


	public Remove(DataInputStream input) throws IOException { 	
		super(input);
		siteId=input.readInt();
		objSeqNo=input.readInt();
		sizeOfId=input.readInt();
		byte[] ObjIdbyte=new byte[sizeOfId];
		input.readFully(ObjIdbyte);
		this.ObjectId=new String(ObjIdbyte,"UTF-8") ; 
	}



	public MessageType getType() {
		return MessageType.Remove;
	}

	public int byteSize() {
		return  super.byteSize()+4+4+4+ObjectId.length();
	}


	public String toString() {
		return "Remove(" + super.toString() +")";
	}


	protected void write(ByteBuffer bb) {		
		bb.putInt(siteId);
		bb.putInt(objSeqNo);	
		bb.putInt(sizeOfId);
		bb.put(ObjectId.getBytes());
	}

}