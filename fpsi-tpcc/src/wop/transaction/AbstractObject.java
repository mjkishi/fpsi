package wop.transaction;



public abstract class AbstractObject {
	 public static long MAX_VERSION_NUMBER = 9223372036854775807L;
	  public long version;
	  public int[] vc;
	  public int sizeOfVC;
	  
	 
	  
	  
	  
	  public void setVC(int[] commitVC) {
		  this.vc=commitVC;
	  }
  
	  
	  
	  
	  public AbstractObject(int sizeOfVC) {
		  version = 0; 
		  this.sizeOfVC=sizeOfVC;
		  vc=new int[sizeOfVC];
		  
	  }
	  
	  public abstract String getId();
	 
	  public long getVersion() {
		  return version;
	  }
	  
	  
	  public void setVersion(long nextversion) {
		  version = nextversion;
	  }

	  public void incrementVersion() {
		  version += 1;
	  }
	  
}
