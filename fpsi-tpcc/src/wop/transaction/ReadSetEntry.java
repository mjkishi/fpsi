package wop.transaction;

public class ReadSetEntry {
	private AbstractObject object;
	private int siteId;
	private int objSeqNo;

	public ReadSetEntry(AbstractObject object, int siteId, int objSeqNo) {
		this.object=object;
		this.siteId=siteId;
		this.objSeqNo=objSeqNo;

	}

	public int getSiteId() {
		return this.siteId;
	}

	public int getObjectSeqNo() {
		return this.objSeqNo;
	}

	public AbstractObject getObject() {
		return this.object;
	}



}
