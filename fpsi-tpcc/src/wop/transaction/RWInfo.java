package wop.transaction;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import wcc.common.RequestId;

public class RWInfo {
	public Set<RequestId> mainReqs;


	public RWInfo() {
		mainReqs=new ConcurrentSkipListSet<RequestId>();
	}

}
