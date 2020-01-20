package wop.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import wcc.common.RequestId;



public abstract class Message implements Serializable {

	private static final long serialVersionUID = 1L;   
	private RequestId requestId ;



	protected Message(RequestId requestId) {
		this.requestId=requestId;
	}


	protected Message(DataInputStream input) throws IOException {
		this.requestId = new RequestId(input);
	}

	public RequestId getRequestId() {
		return requestId;
	}

	public int byteSize() {
		return  1+4 +8;
	}

	public String toString() {
		return "RequestId:" + requestId.toString();
	}    

	public final byte[] toByteArray() {

		ByteBuffer bb = ByteBuffer.allocate(byteSize());
		bb.put((byte) getType().ordinal());
		bb.put(requestId.toByteArray());
		write(bb);
		assert bb.remaining() == 0 : "Wrong sizes. Limit=" + bb.limit() + ",capacity=" +
				bb.capacity() + ",position=" + bb.position();
		return bb.array();

	}

	public abstract MessageType getType();


	protected abstract void write(ByteBuffer bb);
}
