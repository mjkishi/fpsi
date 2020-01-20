package wop.objectStructure;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerArray;

import wcc.common.RequestId;

public class VersionBound {
	public ConcurrentMap<RequestId, AtomicIntegerArray> versionBound=new ConcurrentHashMap<RequestId, AtomicIntegerArray>();


	public  ConcurrentMap<RequestId, AtomicIntegerArray> getVersionBound(){
		return this.versionBound;
	}


}
