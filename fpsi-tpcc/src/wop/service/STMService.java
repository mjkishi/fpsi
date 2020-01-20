package wop.service;

import java.io.IOException;
import java.util.Map;

import wop.TwoPhaseCommit.replica.Node;
import wop.transaction.AbstractObject;
import wop.transaction.TransactionContext;



public abstract class STMService {

    public abstract Node getNode();	
    
	public abstract byte[] serializeObject(AbstractObject object) throws IOException;
		
	public abstract AbstractObject deserializeObject(String Id,byte[] bytes);
	
	public abstract byte[] serializeWriteSet(TransactionContext ctx);
			
	public abstract Map<String,AbstractObject> deserializeWriteSet(int wsize,byte[] bytes);
	
	public abstract void IncreaseCommitCount();
	
	public abstract void addToCollectedNum(int collectedNum);

	public abstract byte[] serializeReadSet(TransactionContext ctx) ;

	public abstract Map<String, AbstractObject> deserializeReadSet(int rsize, byte[] bytes) ;
	
}