package wcc;

import java.util.BitSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import wcc.common.RequestId;
import wop.objectStructure.ObjectHistory;
import wop.transaction.AbstractObject;



public class Store {
	private ConcurrentMap<String, ObjectHistory> store;
	private ConcurrentMap<String, BitSet> containers;
	int replicationDegree;
	private int accessibleObjects;
	private int[] partitions;
	int localId;
	NodeStatus nodestatus;



	public Store(int capacity, int localId, int numNodes,int replicationDegree) {
		store=new ConcurrentHashMap<String, ObjectHistory>(capacity);
		containers = new ConcurrentHashMap<String, BitSet>(capacity);
		this.replicationDegree=replicationDegree;
		this.accessibleObjects = capacity / numNodes;
		this.partitions=new int[replicationDegree];
		this.localId=localId;

	}



	public void setNodeStatus(NodeStatus nodestatus) {
		this.nodestatus=nodestatus;
	}


	public int getAccessibleObject() {
		return this.accessibleObjects;
	}


	public int getReplicationDegree() {
		return this.replicationDegree;
	}


	public void registerPartitions(int numNodes, int localId) {
		int firstPartitionNo=localId;
		partitions[0]=firstPartitionNo;
		int nextPartition=firstPartitionNo;
		for(int i=1; i< this.replicationDegree;i++) {
			if(nextPartition<numNodes-1) {
				nextPartition=nextPartition+1;
				partitions[i]=nextPartition;
			}
			else {
				if(nextPartition==numNodes-1) {
					nextPartition=0;
					partitions[i]=nextPartition;
				}
				else {
					nextPartition=nextPartition+1;
					partitions[i]=nextPartition;
				}
			}
		}
		for(int i=0; i< this.replicationDegree;i++) {
			System.out.println("For node"+localId+" partitions are "+partitions[i]);
		}


	}




	public void registerObjectToStore(String Id,int id, AbstractObject object, int numNodes, int localId){
		for(int i=0; i<this.replicationDegree;i++) {
			if((id+i)%numNodes==localId )
			{
				ObjectHistory objecthistory=new  ObjectHistory(numNodes);
				objecthistory.registerToH(object, numNodes);
				store.put(Id, objecthistory);
			}

			if(!containers.containsKey(Id)) {
				BitSet replicas = new BitSet(numNodes);
				replicas.set((id+i) % numNodes);
				containers.put(Id, replicas);
			}
			else {
				BitSet replicas=containers.get(Id);
				replicas.set((id+i) % numNodes);
				containers.put(Id, replicas);
			}

		}
		/* for(String s:containers.keySet() ) {
         System.out.println("Destination for ID "+s+" is "+containers.get(s));
     }
		 */
	}




	public void registerObjectToStoreForLocality(String k, AbstractObject object, int id, int localId, int numNodes, int capacity) {
		this.accessibleObjects = capacity / numNodes;
		int firstPartitionNo=(int)Math.ceil((double)(id/accessibleObjects));
		BitSet replicas;
		if(!containers.containsKey(k)) {
			replicas= new BitSet(numNodes);
			replicas.set(firstPartitionNo);
			int nextPartition=firstPartitionNo;
			for(int i=1; i< this.replicationDegree;i++) {
				if(nextPartition<numNodes-1) {
					nextPartition=nextPartition+1;
					partitions[i]=nextPartition;
				}
				else {
					if(nextPartition==numNodes-1) {
						nextPartition=0;
						partitions[i]=nextPartition;
					}
					else {
						nextPartition=nextPartition+1;
						partitions[i]=nextPartition;
					}
				}
				replicas.set(partitions[i]);
			}
		}
		else {
			replicas=containers.get(k); 
			replicas.set(firstPartitionNo);


			int nextPartition=firstPartitionNo;
			for(int i=1; i< this.replicationDegree;i++) {
				if(nextPartition<numNodes-1) {
					nextPartition=nextPartition+1;
					partitions[i]=nextPartition;
				}
				else {
					if(nextPartition==numNodes-1) {
						nextPartition=0;
						partitions[i]=nextPartition;
					}
					else {
						nextPartition=nextPartition+1;
						partitions[i]=nextPartition;
					}
				}
				replicas.set(partitions[i]);
			}			 
		}

		for(int i=0; i<this.replicationDegree;i++) {
			if(this.partitions[i]==firstPartitionNo) {
				for(int j=0;j<this.replicationDegree;j++) {
					replicas.set(this.partitions[j]);
					break;
				}
			}		 
		}

		for(int i=0;i<this.replicationDegree;i++) {
			if(replicas.get(localId)) {
				ObjectHistory objecthistory=new  ObjectHistory(numNodes);
				objecthistory.registerToH(object, numNodes);
				store.put(k, objecthistory); 
			}

		}
		containers.put(k, replicas);
		// System.out.println(" for object "+k+" containers are  "+replicas);
	}


	public void registerToStore(String k, String w_id,AbstractObject object, int localId, int numNodes) {
		BitSet replicas=containers.get(w_id);
		for (int i = replicas.nextSetBit(0); i >= 0; i = replicas.nextSetBit(i + 1)) {
			if(i==localId) {
				ObjectHistory objecthistory=new  ObjectHistory(numNodes);
				objecthistory.registerToH(object, numNodes);
				store.put(k, objecthistory); 	
			}
		}
		containers.put(k, replicas);
		//System.out.println(" for object "+k+" containers are  "+replicas);
	}


	public void registerObjectToStoreForLocality(String k, AbstractObject object, int id, int localId, int numNodes) {
		int firstPartitionNo=(int)Math.ceil((double)(id/accessibleObjects));
		BitSet replicas;
		if(!containers.containsKey(k)) {
			replicas= new BitSet(numNodes);
			replicas.set(firstPartitionNo);
			int nextPartition=firstPartitionNo;
			for(int i=1; i< this.replicationDegree;i++) {
				if(nextPartition<numNodes-1) {
					nextPartition=nextPartition+1;
					partitions[i]=nextPartition;
				}
				else {
					if(nextPartition==numNodes-1) {
						nextPartition=0;
						partitions[i]=nextPartition;
					}
					else {
						nextPartition=nextPartition+1;
						partitions[i]=nextPartition;
					}
				}
				replicas.set(partitions[i]);
			}
		}
		else {
			replicas=containers.get(k);
			replicas.set(firstPartitionNo);
			int nextPartition=firstPartitionNo;
			for(int i=1; i< this.replicationDegree;i++) {
				if(nextPartition<numNodes-1) {
					nextPartition=nextPartition+1;
					partitions[i]=nextPartition;
				}
				else {
					if(nextPartition==numNodes-1) {
						nextPartition=0;
						partitions[i]=nextPartition;
					}
					else {
						nextPartition=nextPartition+1;
						partitions[i]=nextPartition;
					}
				}
				replicas.set(partitions[i]);
			}
		}

		for(int i=0; i<this.replicationDegree;i++) {
			if(this.partitions[i]==firstPartitionNo) {
				for(int j=0;j<this.replicationDegree;j++) {
					replicas.set(this.partitions[j]);
					break;
				}
			}
		}

		for(int i=0;i<this.replicationDegree;i++) {
			if(replicas.get(localId)) {
				ObjectHistory objecthistory=new  ObjectHistory(numNodes);
				objecthistory.registerToH(object, numNodes);
				store.put(k, objecthistory);
			}
		}
		containers.put(k, replicas);
		//MA  System.out.println(" for account "+k+" containers are  "+replicas);
	}



	public BitSet getContainers(String k) {
		return containers.get(k);
	}


	public ReadResponse getLatestObject1(RequestId reqId,String Id,int [] view, int[] hasRead, int numNodes, int isUpdate, int reader){	
		//	System.out.println("getLatestObject1 is called for requestId "+reqId+" and for Id "+Id);
		AbstractObject obj=null;
		int siteId=0;
		int objSeqNo=0;
		if(isUpdate==1) {
			while(!store.get(Id).LockHistory(Id, reader, reqId)) {}
			long version=0;
			long nextversion=0;
			int seqNo=0;
			Map.Entry<Integer,AbstractObject> ent;
			int sn=0;
			for(int i=0; i<numNodes;i++){
				obj= store.get(Id).getSitehistory(i).getLatestObject();
				seqNo=store.get(Id).getSitehistory(i).getLatestSeqNo();
				if(hasRead[i]==1) {
					seqNo=view[i];
				}
				if(seqNo<0) {
					seqNo=0;
				}
				ent= store.get(Id).getSitehistory(i).getLatestObject(seqNo);
				obj=ent.getValue();
				nextversion=obj.getVersion();		
				for(int j=0;j<numNodes; j++) {
					if(hasRead[j]==1&& obj.vc[j]>view[j]) {
						int index=0;
						do {									
							obj= store.get(Id).getSitehistory(i).getLatestObject(seqNo/*view[i]*/-index).getValue();							
							sn= store.get(Id).getSitehistory(i).getLatestObject(seqNo/*view[i]*/-index).getKey();
							index++;
							System.out.println("ReqId :::::::: "+reqId+" is printing index as "+index+" and siteId is "+
									siteId+" looking fotr version "+version);
						}while(obj.vc[j]>view[j] );
						nextversion=obj.getVersion();		
						seqNo=sn;
					}
				}		
				if(i==0) {
					version=ent.getValue().getVersion();
					nextversion=ent.getValue().getVersion();
				}		
				int count =0;
				if(hasRead[i]==0 ) {
					for (int l=0;l<numNodes;l++) {
						if(hasRead[l]==1 && ent.getValue().vc[l]==view[l]) {
							count++;
						}
					}
					if(count ==numNodes-1 && seqNo!=0 && seqNo>view[i]) {
						obj= store.get(Id).getSitehistory(i).getLatestObject(seqNo-1/*view[i]*/).getValue();							
						sn= store.get(Id).getSitehistory(i).getLatestObject(seqNo-1/*view[i]*/).getKey();
						nextversion=obj.getVersion();		
						seqNo=sn;
					}	
				}

				view[i]=seqNo;	
				if(nextversion>version){
					obj=ent.getValue();
					version=nextversion;
					sn= store.get(Id).getSitehistory(i).getLatestObject(view[i]).getKey();
					siteId=i;
					view[i]=sn;
				}
				if(nextversion<version){
					obj= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]).getValue();
					version=obj.getVersion();
				}

				if(nextversion==0 && nextversion==version) {
					obj= store.get(Id).getSitehistory(i).getLatestObject(0).getValue();
					version=obj.getVersion();
					siteId=i;
				}
			}
			store.get(Id).UnLockedbyLocker(reader, reqId, Id, "Read");	
		}
		///////////////////////////////////////////////////////////////////////////read-only /////////////////////////////////////////////////////////
		else {
			while(!store.get(Id).LockHistory(Id, reader, reqId)) {}
			boolean go=true;
			long version=0;
			long nextversion=0;			
			int firstView=0;
			int seqNo=0;	
			Map.Entry<Integer,AbstractObject> ent;
			int sn=0;
			for(int i=0; i<numNodes;i++){			
				obj= store.get(Id).getSitehistory(i).getLatestObject();
				seqNo=store.get(Id).getSitehistory(i).getLatestSeqNo();
				objSeqNo=seqNo;
				firstView=view[i];
				if(hasRead[i]==1) {
					seqNo=view[i];
				}                    
				if(hasRead[i]==0) {
					boolean stop;
					do {
						stop=true;
						for(int l=0;l<numNodes;l++) {
							if(hasRead[l]==1 && obj.vc[l]>view[l]) {
								seqNo=seqNo-1;
								if(seqNo<0) {
									seqNo=0;
								}
								ent=store.get(Id).getSitehistory(i).getLatestObject(seqNo);
								view[i]=seqNo;
								obj=ent.getValue();
								objSeqNo=ent.getKey();
								nextversion=obj.version;
								stop=false;
								break;
							}
						}
					}while(!stop);
				}
				ent= store.get(Id).getSitehistory(i).getLatestObject(seqNo);
				if(i==0) {
					version=ent.getValue().getVersion();
					obj=ent.getValue();
					objSeqNo=ent.getKey();
					nextversion=version;
				}
				else {
					nextversion=ent.getValue().getVersion();
				}
				int lastSeqNo=store.get(Id).getSitehistory(i).getLatestObject(seqNo).getKey();
				if(isUpdate == 0  && hasRead[i]==0 && store.get(Id).getSitehistory(i).getMainRWInfo(Id, lastSeqNo).contains(reqId)) {
					seqNo=lastSeqNo-1;
					if(seqNo<0) {
						seqNo=0;
					}
					lastSeqNo=store.get(Id).getSitehistory(i).getLatestObject(seqNo).getKey();					
					while(store.get(Id).getSitehistory(i).getMainRWInfo(Id, lastSeqNo).contains(reqId)) {
						seqNo=lastSeqNo-1;
						if(seqNo<0) {
							seqNo=0;
						}

						if(lastSeqNo-1<0) {
							lastSeqNo=1;
						}

						lastSeqNo=store.get(Id).getSitehistory(i).getLatestObject(lastSeqNo-1).getKey(); 				
					}		
					if(seqNo<0) {
						seqNo=0;
					}
					go=false;
				}
				view[i]=seqNo;
				obj=store.get(Id).getSitehistory(i).getLatestObject(view[i]).getValue();
				objSeqNo=store.get(Id).getSitehistory(i).getLatestPossibleSeqNo(view[i]);
				nextversion=obj.version;
				if(i!=0&&go && nextversion>version){
					sn= Math.max(store.get(Id).getSitehistory(i).getLatestObject(view[i]).getKey(), view[i]);
					if(hasRead[i]==0) {
						view[i]=sn;
					}
					boolean visibility=true;
					for(int j=0; j<numNodes;j++) {
						if(hasRead[j]==1 && obj.vc[j]>view[j]) {
							if(store.get(Id).getSitehistory(i).getLatestObject(view[i]-1).getValue().getVersion()>version) {
								obj=store.get(Id).getSitehistory(i).getLatestObject(view[i]-1).getValue();
								nextversion=obj.getVersion();                          
								sn= store.get(Id).getSitehistory(i).getLatestObject(view[i]-1).getKey();//c
								objSeqNo=sn;
								view[i]=sn;
								boolean goBack;
								do {
									goBack=false;
									for(int k=0; k<numNodes;k++) {
										if(hasRead[k]==1 && obj.vc[k]>view[k]) {		
											view[i]=view[i]-1;
											obj=store.get(Id).getSitehistory(i).getLatestObject(view[i]).getValue();
											objSeqNo=store.get(Id).getSitehistory(i).getLatestPossibleSeqNo(view[i]);
											nextversion=obj.version;	
											goBack=true;
										}
									}

								}while(goBack);										
								if(nextversion>version) {
									siteId=i;
									obj=store.get(Id).getSitehistory(i).getLatestObject(view[i]).getValue();
									objSeqNo=store.get(Id).getSitehistory(i).getLatestPossibleSeqNo(view[i]);
									version=obj.version;
								}
								else {
									int index=0;
									do {
										obj= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getValue();
										index++;
										objSeqNo= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getKey();
									}while(obj.version!=version);
									version=obj.getVersion();										
								}
							}

							else {
								int index=0;
								do {
									obj= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getValue();
									index++;
									objSeqNo= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getKey();
								}while(obj.version!=version);
								version=obj.getVersion();					
							}
							visibility=false;
						}
						if(visibility ) {
							for (int k=0; k<numNodes;k++) {
								if(hasRead[k]==0) {
									view[k]=Math.max(view[k], obj.vc[k]);
								}
							}
						}
					}				
					if(visibility) {
						version=nextversion;
						siteId=i;
					}
				}		
				else if(i!=0&& go && nextversion==version) {
					obj=ent.getValue();
					objSeqNo=ent.getKey();
					sn= store.get(Id).getSitehistory(i).getLatestObject(0).getKey();
					siteId=i;
					view[i]=Math.max(firstView, view[i]);
				}

				else if(i!=0&& go && nextversion<version) {
					int index=0;
					do {
						obj= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getValue();
						objSeqNo= store.get(Id).getSitehistory(siteId).getLatestPossibleSeqNo(view[siteId]-index);
						index=index+1;
					}while(obj.version!=version);
					version=obj.getVersion();
					if(hasRead[i]==0) {
						view[i]=Math.max(firstView, view[i]);
					}				
				}

				else if(!go) {
					hasRead[i]=1;
					ReadResponse r=getLatestObject2(reqId,Id,view, hasRead,numNodes,isUpdate);
					obj=r.getObject();
					break;
				}				
				if(hasRead[i]==1) {
					view[i]=seqNo;
				}
			}
			///End of version selection
			if(go &&isUpdate==0) {
				int addedseqNo=store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]).getKey();
				synchronized(store.get(Id).getSitehistory(siteId).getMainRWInfo(Id, addedseqNo)) {
					store.get(Id).getSitehistory(siteId).addMainRWInfo(reqId, Id,addedseqNo, siteId," getLatestObject1");
				}
			}
			for(int i=0; i<numNodes;i++){
				if(isUpdate==0) {
					view[i]=Math.max(view[i], obj.vc[i]);
				}
			}		
			store.get(Id).UnLockedbyLocker(reader, reqId, Id, "Read");


		}
		return new ReadResponse(obj,view, siteId, objSeqNo);
	}





	public ReadResponse getLatestObject2(RequestId reqId,String Id,int [] view, int[] hasRead, int numNodes, int isUpdate){
		AbstractObject obj=null;
		long version=0;
		long nextversion=0;
		int seqNo=0;
		int siteId=0;
		int firstView=0;
		int objSeqNo=0;
		Map.Entry<Integer,AbstractObject> ent;
		int sn=0;
		for(int i=0; i<numNodes;i++){
			obj= store.get(Id).getSitehistory(i).getLatestObject();
			seqNo=store.get(Id).getSitehistory(i).getLatestSeqNo();
			objSeqNo=seqNo;
			firstView=view[i];
			if(hasRead[i]==1) {
				seqNo=view[i];
			}
			if(seqNo<0) {
				seqNo=0;
			}
			ent= store.get(Id).getSitehistory(i).getLatestObject(seqNo);
			///****************///
			if(i==0) {
				version=ent.getValue().getVersion();
				obj=ent.getValue();
				objSeqNo=ent.getKey();
				nextversion=version;
			}
			else {
				nextversion=ent.getValue().getVersion();
			}
			view[i]=seqNo;
			ent= store.get(Id).getSitehistory(i).getLatestObject(seqNo);
			if(hasRead[i]==0) {
				boolean stop;
				do {
					stop=true;
					for(int l=0;l<numNodes;l++) {
						if(hasRead[l]==1 && obj.vc[l]>view[l]) {
							seqNo=seqNo-1;
							if(seqNo<0) {
								seqNo=0;
							}
							ent=store.get(Id).getSitehistory(i).getLatestObject(seqNo);
							view[i]=seqNo;
							obj=ent.getValue();
							objSeqNo=ent.getKey();
							nextversion=obj.version;
							stop=false;
							break;
						}
					}
				}while(!stop);
			}
			boolean is_set=false;
			if(  nextversion>version ){
				obj=ent.getValue();
				objSeqNo=ent.getKey();
				sn= store.get(Id).getSitehistory(i).getLatestObject(view[i]).getKey();
				while(store.get(Id).getSitehistory(i).getMainRWInfo(Id, sn).contains(reqId)) {
					if(store.get(Id).getSitehistory(i).getLatestObject(sn-1).getValue().getVersion()<version) {
						sn=store.get(Id).getSitehistory(i).getLatestObject(sn-1).getKey();
						int index=0;
						do {
							obj= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getValue();
							objSeqNo=store.get(Id).getSitehistory(siteId).getLatestPossibleSeqNo(view[siteId]-index);
							index++;
						}while(obj.version!=version);
						version=obj.getVersion();
						is_set=true;
						break;
					}
					else {
						sn=store.get(Id).getSitehistory(i).getLatestObject(sn-1).getKey();
						obj=store.get(Id).getSitehistory(i).getLatestObject(sn).getValue();
						objSeqNo=store.get(Id).getSitehistory(i).getLatestPossibleSeqNo(sn);
						nextversion=obj.getVersion();
					}
				}
				boolean visibility=true;
				for(int j=0; j<numNodes; j++) {
					if(hasRead[j]==1 && obj.vc[j]>view[j]) {
						if(store.get(Id).getSitehistory(i).getLatestObject(view[i]-1).getValue().getVersion()>version) {
							obj=store.get(Id).getSitehistory(i).getLatestObject(view[i]-1).getValue();
							objSeqNo=store.get(Id).getSitehistory(i).getLatestPossibleSeqNo(view[i]-1);
							nextversion=obj.getVersion();
							sn= store.get(Id).getSitehistory(i).getLatestObject(view[i]-1).getKey();//c
							view[i]=sn;
							boolean goBack;
							do {
								goBack=false;
								for(int k=0; k<numNodes;k++) {
									if(hasRead[k]==1 && obj.vc[k]>view[k]) {		
										view[i]=view[i]-1;
										obj=store.get(Id).getSitehistory(i).getLatestObject(view[i]).getValue();
										objSeqNo=store.get(Id).getSitehistory(i).getLatestPossibleSeqNo(view[i]);
										nextversion=obj.version;	
										goBack=true;
									}
								}
							}while(goBack);
							if(nextversion>version) {
								siteId=i;
								obj=store.get(Id).getSitehistory(i).getLatestObject(view[i]).getValue();
								objSeqNo=store.get(Id).getSitehistory(i).getLatestPossibleSeqNo(view[i]);
								version=obj.getVersion();	
							}
							else {								
								int index=0;
								do {				    
									obj= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getValue();
									objSeqNo=store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getKey();
									index++;
								}while(obj.getVersion()!=version);
							}  
						}
						else {
							int index=0;
							do {
								obj= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getValue();
								objSeqNo=store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getKey();
								index++;
							}while(obj.getVersion()!=version);
						}
						visibility=false;
					}
				}
				if(visibility && !is_set  ) {
					version=nextversion;
					siteId=i;
				}
			}
			else if(i!=0&& nextversion<version) {
				int index=0;
				do {
					obj= store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getValue();
					objSeqNo=store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]-index).getKey();
					index++;
				}while(obj.version!=version);
				version=obj.getVersion();
			}
			else if(i!=0 && nextversion==version) {
				obj= store.get(Id).getSitehistory(i).getLatestObject(0).getValue();
				objSeqNo=0;
				version=obj.getVersion();
				siteId=i;
				if(hasRead[i]==1) {
					view[i]=Math.max(firstView, view[i]); 
				}
				if(hasRead[i]==0) {
					view[i]=0;
				}
			}
			if(hasRead[i]==1) {
				view[i]=seqNo;
			}
		}
		if(isUpdate==0) {
			int addedseqNo=store.get(Id).getSitehistory(siteId).getLatestObject(view[siteId]).getKey();
			synchronized(store.get(Id).getSitehistory(siteId).getMainRWInfo(Id, addedseqNo)) {
				store.get(Id).getSitehistory(siteId).addMainRWInfo(reqId, Id,addedseqNo, siteId," getLatestObject2");
			}
		}	
		return new ReadResponse(obj,view,siteId,objSeqNo);
	}


	public AbstractObject getLAtestCommitted(String Id){
		return store.get(Id).getObject(getLastVersion(Id).getlocalId());
	}


	public ObjectHistory getObjectHistory(String Id){
		return store.get(Id);
	}


	public boolean Ifreplicate(String objId){
		if (store.containsKey(objId)){
			return true;
		}
		else {
			return false;
		}
	}

	public Version getLastVersion(String Id){
		return store.get(Id).getLatestVersion();
	}



	public void updateStore(RequestId reqId, int updator, String ObjId, int sn,AbstractObject object/*, int[] VC*/, String line) {
		store.get(ObjId).addObjecthistory(reqId, ObjId,object, sn, updator/*,VC*/,line);
	}



	public ConcurrentMap<String, ObjectHistory> getStore(){
		return store;
	}
}