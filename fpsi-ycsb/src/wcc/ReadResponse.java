package wcc;


import wop.transaction.AbstractObject;

public class ReadResponse {
	AbstractObject object;
	int[]  suggestedVC;
	int siteId;
	int objSeqNo;


	public ReadResponse(AbstractObject object,int[] suggestedVC, int siteId, int objSeqNo) {
		this.object=object;
		this.suggestedVC=suggestedVC;
		this.siteId=siteId;
		this.objSeqNo=objSeqNo;
	}

	public AbstractObject getObject() {
		return this.object;
	}

	public int[] getSuggestedVC(){
		return this.suggestedVC;
	}
	
	public int getSiteId() {
		return this.siteId;
	}
	
	public int getObjSeqNo() {
		return this.objSeqNo;
	}

}