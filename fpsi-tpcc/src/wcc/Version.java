package wcc;



import wop.transaction.AbstractObject;

public class Version {
	private int localId;
	private int seqNo;
	public volatile AbstractObject obj;
    public long version;
	
	public Version(AbstractObject obj, int numNodes){
	  this.localId=numNodes-1;
	  this.seqNo=0;
	  this.obj=obj;
	  this.version=obj.getVersion();
	}

	
	public long getVersion(){
		synchronized(this) {
		return version;
		}
	}
		
	public void SetlocalId(int localId){
		this.localId=localId;
	}
	
	
	public void SetSeqNo(int seqNo){
		this.seqNo=seqNo;
	}
	
	public void setObjec(AbstractObject obj){
		this.obj=obj;
	}
	
	public AbstractObject getObject(){
		return this.obj;
	}
	
	
	public long getLatestVersionNum(){
		return obj.getVersion();
	}
	
	
	public void SetNo(int seqNo){
		this.seqNo=seqNo;
	}
	
	public int getlocalId(){
		return this.localId;
	}
	
	
	public int getSeqNo(){
		return this.seqNo;
	}
	
}