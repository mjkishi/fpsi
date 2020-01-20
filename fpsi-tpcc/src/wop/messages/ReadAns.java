package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import wcc.common.RequestId;


public final class ReadAns extends Message {
	private static final long serialVersionUID = 1L;
	private int sizeOfId;
	private String ObjectId;
	private int sizeOfObject;
	private final byte[] Obj;
	private byte[] StartVTS;
	private int numNodes;
	private int retryTimes;
	private int isUpdate;
	private int siteId;
	private int objSeqNo;




	public ReadAns(RequestId requestId, int sizeOfId,String ObjectId,int sizeOfObject,byte[] Obj, byte[] StartVTS, int numNodes , int retryTimes, int isUpdate, int siteId, int objSeqNo) {
		super(requestId);
		this.sizeOfId=ObjectId.length();
		this.ObjectId=ObjectId;
		this.sizeOfObject=sizeOfObject;
		assert Obj != null;
		this.Obj =Obj;
		assert StartVTS!=null;
		this.StartVTS=StartVTS;
		this.numNodes=numNodes;
		this.retryTimes=retryTimes;
		this.isUpdate=isUpdate;
		this.siteId=siteId;
		this.objSeqNo=objSeqNo;
	}

	public RequestId getRequestId(){
		return super.getRequestId();
	}

	public String getObjectId(){
		return ObjectId;
	}

	public byte[] getObj(){
		return Obj;
	}

	public byte[] getStartVTS(){
		return this.StartVTS;
	}

	public int getRetryTimes() {
		return this.retryTimes;
	}


	public int getIsUpdate() {
		return this.isUpdate;
	}

	public int getSiteId() {
		return this.siteId;
	}

	public int getObjSeqNo() {
		return this.objSeqNo;
	}

	public ReadAns(DataInputStream input) throws IOException {
		super(input);
		this.sizeOfId=input.readInt();
		byte[] ObjIdbyte=new byte[sizeOfId];
		input.readFully(ObjIdbyte);
		this.ObjectId=new String(ObjIdbyte,"UTF-8") ;
		this.sizeOfObject=input.readInt();
		Obj=new byte[sizeOfObject]; 
		input.readFully(Obj);
		numNodes=input.readInt();
		StartVTS=new byte[numNodes*4];
		input.readFully(StartVTS);
		retryTimes=input.readInt();
		isUpdate=input.readInt();
		siteId=input.readInt();
		objSeqNo=input.readInt();
	}

	public ReadAns(Read msg, int sizeOfObject, byte[] Obj, byte[] StartVTS, int numNodes, int retryTimes, int isUpdate, int siteId, int objSeqNo) {
		super(msg.getRequestId());
		this.sizeOfId=msg.getSizeId();
		this.ObjectId=msg.getObjectId();
		this.sizeOfObject=sizeOfObject;
		this.Obj=Obj;
		this.StartVTS=StartVTS;
		this.numNodes=numNodes;
		this.retryTimes=retryTimes;
		this.isUpdate=isUpdate;
		this.siteId=siteId;
		this.objSeqNo=objSeqNo;
	}

	public MessageType getType() {
		return MessageType.ReadAns;
	}

	public int byteSize() {
		return  super.byteSize() +4+ObjectId.length()+Obj.length+4+StartVTS.length+4+4+4+4+4;
	}


	public int getObjLength(){
		return Obj.length;
	}

	public String toString() {
		return "ReadAns(" + super.toString() +")";
	}


	protected void write(ByteBuffer bb) {
		bb.putInt(sizeOfId);
		bb.put(ObjectId.getBytes());
		bb.putInt(sizeOfObject);
		bb.put(Obj);
		bb.putInt(numNodes);
		bb.put(StartVTS);
		bb.putInt(retryTimes);
		bb.putInt(isUpdate);
		bb.putInt(siteId);
		bb.putInt(objSeqNo);
	}	  
}