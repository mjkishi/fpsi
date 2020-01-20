package wcc.common;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Represents the unique id of request. To ensure uniqueness, the id contains
 * the id of client and the request sequence number. Every client should have
 * assigned unique client id, so that unique requests id can be created. Clients
 * gives consecutive sequence numbers to every sent request. The sequence number
 * starts with 0.
 */
public class RequestId implements Serializable, Comparable<RequestId> {
    private static final long serialVersionUID = 1L;

    /** Represents the no-op request. */
    public static final RequestId NOP = new RequestId(-1, -1);

    private final long clientId;
    private final int seqNumber;
    
    
    
    public RequestId(DataInputStream input) throws IOException{
    	this.clientId=input.readLong();
    	this.seqNumber=input.readInt();
    }

    /**
     * Creates new <code>RequestId</code> instance.
     * 
     * @param clientId - the id of client
     * @param seqNumber - the request sequence number
     */
    public RequestId(long clientId, int seqNumber) {
        this.clientId = clientId;
        this.seqNumber = seqNumber;
    }

    /**
     * Returns the id of client.
     * 
     * @return the id of client
     */
    public Long getClientId() {
        return clientId;
    }

    /**
     * Returns the request sequence number.
     * 
     * @return the request sequence number
     */
    public int getSeqNumber() {
        return seqNumber;
    }

    
     public static RequestId create(ByteBuffer buffer) {
        Long clientId = buffer.getLong();
        int sequenceId = buffer.getInt();   
        return new RequestId(clientId, sequenceId);
    }
    
     public int byteSize() {
         return 8 + 4 ;
     }

     public void writeTo(ByteBuffer bb) {
         bb.putLong(getClientId());
         bb.putInt(getSeqNumber());
         
     }
     
     public byte[] toByteArray() {
         ByteBuffer byteBuffer = ByteBuffer.allocate(byteSize());
         writeTo(byteBuffer);
         return byteBuffer.array();
     }
     
   
    public int compareTo(RequestId requestId) {
    	  if (clientId != requestId.clientId) {
                  return (int)(clientId - requestId.clientId);
          }
          return seqNumber - requestId.seqNumber;
      }
    	
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        RequestId requestId = (RequestId) obj;
        return clientId == requestId.clientId && seqNumber == requestId.seqNumber;
    }

    public int hashCode() {
        return (int) (clientId ^ (clientId >>> 32)) ^ seqNumber;
    }

    public boolean isNop() {
        return clientId == -1 && seqNumber == -1;
    }

    public String toString() {
        return isNop() ? "nop" : clientId + ":" + seqNumber;
    }
}