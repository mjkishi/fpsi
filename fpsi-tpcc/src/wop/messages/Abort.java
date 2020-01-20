package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import wcc.common.RequestId;


public final class Abort extends Message {
	private static final long serialVersionUID = 1L;
	private int wsize;
	private byte[] writeSet;
	private final int objIdbytesize=16;


	public Abort(RequestId requestId,  int wsize,byte[] writeSet) {
		super(requestId);
		this.wsize=wsize;
		this.writeSet=writeSet;
	}

	public RequestId getRequestId(){
		return super.getRequestId();
	}

	public byte[] getwriteSet(){
		return writeSet;
	}

	public int getwsize(){
		return wsize;
	}


	public Abort(DataInputStream input) throws IOException {
		super(input);
		this.wsize=input.readInt();
		this.writeSet=new byte[wsize*objIdbytesize];
		input.readFully(writeSet);

	}

	public Abort(PrepareAns msg, int wsize,byte[] writeSet){
		super(msg.getRequestId());
		this.wsize=wsize;
		this.writeSet=writeSet;
	}

	public MessageType getType() {
		return MessageType.Abort;
	}

	public int byteSize() {
		return  super.byteSize() +4+wsize*objIdbytesize;
	}



	public String toString() {
		return "Abort(" + super.toString() +")";
	}


	protected void write(ByteBuffer bb) {
		bb.putInt(wsize);
		bb.put(writeSet);
	}	  
}