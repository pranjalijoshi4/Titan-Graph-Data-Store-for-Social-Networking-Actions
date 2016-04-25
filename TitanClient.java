package Titan;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.attribute.Text;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class TitanClient extends DB {
	
	private TitanGraph g;
	public static final int SUCCESS = 0;
	public static final int ERROR = -1;
  
	TitanTransaction tx1;

	
	public boolean init() throws DBException {
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
			
			try{
				 	g = TitanFactory.open("/Users/pranjalijoshi/Downloads/titan-0.5.2-hadoop2/conf/titan-cassandra.properties");  
				}
			catch(Exception e)
				{
					System.out.println(e);
					System.exit(0);
				}
			
			
			
		return true;
	}
	
	/*public void cleanup(boolean warmup) throws DBException {
		g.shutdown();
		TitanCleanup.clear(g);
	}*/

	
	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		// TODO Auto-generated method stub
			if (entitySet == null)
				return -1;
			if (entityPK == null)
				return -1;
			
			
	
			tx1 = g.newTransaction();
			
			if (entitySet.equalsIgnoreCase("users")) {
				try{
					//Create a vertex, add entity set and PK 

					//System.out.println(entitySet+" "+entityPK);
			        Vertex user_ver = tx1.addVertexWithLabel("user");
			        user_ver.setProperty("userid",entityPK);
			        user_ver.setProperty("EntitySet",entitySet);
			        
			       
					for (Entry<String, ByteIterator> entry : values.entrySet()) {						
						if (entry.getKey() != "pic" && entry.getKey() != "tpic") {	
							//add properties
							//System.out.println(entry.getKey());
							user_ver.setProperty(entry.getKey(), entry.getValue().toString());
					       // tx1.commit();   
						}
					}
				}
				catch(Exception e)
				{
					System.out.println(e);
					
				}
					
			}
			else if (entitySet.equalsIgnoreCase("resources")) {
				try{

					//Create a vertex, add entity set and PK 
					
					Vertex rsc_ver = tx1.addVertexWithLabel("resource");
					rsc_ver.setProperty("rid",entityPK);
			        
					for (Entry<String, ByteIterator> entry : values.entrySet()) {						
						//add properties
						
						rsc_ver.setProperty(entry.getKey(), entry.getValue().toString());
					}					

				}
				catch(Exception e)
				{
					System.out.println(e);
				}
				
			}
		tx1.commit();
		

		createLinkResource(values, entitySet, entityPK);

		
	      // g.shutdown();
			
		return 0;
	}
	
	public void createLinkResource(HashMap<String, ByteIterator> values, String entitySet,String entityPK){		

		if (entitySet.equalsIgnoreCase("resources") ) {

		for (Entry<String, ByteIterator> entry : values.entrySet()) {						
			TitanTransaction tx = g.newTransaction();

				Iterable it = g.query().has("rid",Compare.EQUAL,entityPK).vertices();
				Vertex r_ver = (Vertex) it.iterator().next();
				r_ver.getProperty("rid");
				
				if(entry.getKey().equals("creatorid")){
					Iterable it1 = g.query().has("userid",Compare.EQUAL,entry.getValue().toString()).vertices();
					Vertex u_ver = (Vertex) it1.iterator().next(); 
					u_ver.getProperty("userid");
					
					u_ver.addEdge("create", r_ver); //Create a link 'create' from user to resource
					r_ver.addEdge("createdFor", u_ver);
					
					tx.commit();
					
				}
				
			}
		}
	}



	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		
		//System.out.println("Hash Map Values");

		int retVal = SUCCESS;
		
		if(retVal<0)
		{
			return ERROR;
		}
		
		
		tx1 = g.newTransaction();
		
		//Get vertex corresponding to profile owner
		String profileowner_id = Integer.toString(profileOwnerID);
		Iterable it = g.query().has("userid",Compare.EQUAL,profileowner_id).vertices();
		Vertex owner = (Vertex) it.iterator().next();
		
		int friendcnt=0;
		Iterable friend_cnt  = owner.getEdges(Direction.OUT, "friendship");
		if(Iterables.size(friend_cnt)!=0)
			friendcnt=Iterables.size(friend_cnt);
		
		//Get create count
		int resourcecnt =0;
		Iterable resource_cnt  = owner.getEdges(Direction.OUT, "create");
		if(Iterables.size(resource_cnt)!=0)
			resourcecnt = Iterables.size(resource_cnt);
		
		int pendingcnt =0;
		
		if(requesterID == profileOwnerID)
		{
			Iterable pending_cnt  = owner.getEdges(Direction.OUT, "pending");
			//Iterable pending_cnt = owner.query().has("user_relationship",Text.CONTAINS,"pending").edges();
			if(Iterables.size(pending_cnt)!=0)
				pendingcnt = Iterables.size(pending_cnt);
		}
		
		//result = new HashMap();
		if(requesterID == profileOwnerID)
			result.put("pendingcount", new ObjectByteIterator(Integer.toString(pendingcnt).getBytes()));
		
		result.put("friendcount", new ObjectByteIterator(Integer.toString(friendcnt).getBytes()));
		result.put("resourcecount", new ObjectByteIterator(Integer.toString(resourcecnt).getBytes()));
		result.put("username", new ObjectByteIterator(String.valueOf(owner.getProperty("username")).getBytes()));
		result.put("pw", new ObjectByteIterator(String.valueOf(owner.getProperty("pw")).getBytes()));
		result.put("fname", new ObjectByteIterator(String.valueOf(owner.getProperty("fname")).getBytes()));
		result.put("gender", new ObjectByteIterator(String.valueOf(owner.getProperty("gender")).getBytes()));
		result.put("dob", new ObjectByteIterator(String.valueOf(owner.getProperty("dob")).getBytes()));
		result.put("jdate", new ObjectByteIterator(String.valueOf(owner.getProperty("jdate")).getBytes()));
		result.put("ldate", new ObjectByteIterator(String.valueOf(owner.getProperty("ldate")).getBytes()));
		result.put("address", new ObjectByteIterator(String.valueOf(owner.getProperty("address")).getBytes()));
		result.put("email", new ObjectByteIterator(String.valueOf(owner.getProperty("email")).getBytes()));
		result.put("tel", new ObjectByteIterator(String.valueOf(owner.getProperty("tel")).getBytes()));
		tx1.commit();
		return retVal;
		
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		// TODO Auto-generated method stub
		// If id's don't exist then return failure
		if (requesterID < 0 || profileOwnerID < 0)
			return ERROR;	
		
		
			String profileowner_id = Integer.toString(profileOwnerID);
			Iterable it = g.query().has("userid",Compare.EQUAL,profileowner_id).vertices();
			Vertex owner = (Vertex) it.iterator().next();
	
		       for(Edge edge : g.getVertex(owner.getId()).getEdges(Direction.OUT, "friendship"))
		       {
		       	tx1=g.newTransaction();
		       	Vertex v=edge.getVertex(Direction.IN);
		       	int friendcnt=0;
		   		Iterable friend_cnt = v.query().has("user_relationship",Text.CONTAINS,"friendship").edges();
		   		if(Iterables.size(friend_cnt)!=0)
		   			friendcnt=Iterables.size(friend_cnt);

		   		
		   		//Get create count
		   		int resourcecnt =0;
		   		Iterable resource_cnt = v.query().has("src_relationship",Text.CONTAINS,"create").edges();
		   		if(Iterables.size(resource_cnt)!=0)
		   			resourcecnt = Iterables.size(resource_cnt);
		   		
		   		
		   		HashMap <String, ByteIterator>re = new HashMap<String, ByteIterator>();
		   		
		   		
		   		re.put("friendcount", new ObjectByteIterator(Integer.toString(friendcnt).getBytes()));
		   		re.put("resourcecount", new ObjectByteIterator(Integer.toString(resourcecnt).getBytes()));
		   		re.put("username", new ObjectByteIterator(String.valueOf(v.getProperty("username")).getBytes()));
		   		//re.put("pw", new ObjectByteIterator(String.valueOf(v.getProperty("pw")).getBytes()));
		   		re.put("fname", new ObjectByteIterator(String.valueOf(v.getProperty("fname")).getBytes()));
		   		re.put("gender", new ObjectByteIterator(String.valueOf(v.getProperty("gender")).getBytes()));
		   		re.put("dob", new ObjectByteIterator(String.valueOf(v.getProperty("dob")).getBytes()));
		   		re.put("jdate", new ObjectByteIterator(String.valueOf(v.getProperty("jdate")).getBytes()));
		   		re.put("ldate", new ObjectByteIterator(String.valueOf(v.getProperty("ldate")).getBytes()));
		   		re.put("address", new ObjectByteIterator(String.valueOf(v.getProperty("address")).getBytes()));
		   		re.put("email", new ObjectByteIterator(String.valueOf(v.getProperty("email")).getBytes()));
		   		re.put("tel", new ObjectByteIterator(String.valueOf(v.getProperty("tel")).getBytes()));
		   		
		   		result.add(re);
		   		tx1.commit();
		       }
	
		       for(Edge edge : g.getVertex(owner.getId()).getEdges(Direction.IN, "friendship"))
		       {
		       	tx1=g.newTransaction();
		       	Vertex v=edge.getVertex(Direction.OUT);
		       	int friendcnt=0;
		   		Iterable friend_cnt = v.query().has("user_relationship",Text.CONTAINS,"friendship").edges();
		   		if(Iterables.size(friend_cnt)!=0)
		   			friendcnt=Iterables.size(friend_cnt);

		   		
		   		//Get create count
		   		int resourcecnt =0;
		   		Iterable resource_cnt = v.query().has("src_relationship",Text.CONTAINS,"create").edges();
		   		if(Iterables.size(resource_cnt)!=0)
		   			resourcecnt = Iterables.size(resource_cnt);
		   		
		   		
		   		HashMap <String, ByteIterator>re = new HashMap<String, ByteIterator>();
		   		
		   		
		   		re.put("friendcount", new ObjectByteIterator(Integer.toString(friendcnt).getBytes()));
		   		re.put("resourcecount", new ObjectByteIterator(Integer.toString(resourcecnt).getBytes()));
		   		re.put("username", new ObjectByteIterator(String.valueOf(v.getProperty("username")).getBytes()));
		   		re.put("fname", new ObjectByteIterator(String.valueOf(v.getProperty("fname")).getBytes()));
		   		re.put("gender", new ObjectByteIterator(String.valueOf(v.getProperty("gender")).getBytes()));
		   		re.put("dob", new ObjectByteIterator(String.valueOf(v.getProperty("dob")).getBytes()));
		   		re.put("jdate", new ObjectByteIterator(String.valueOf(v.getProperty("jdate")).getBytes()));
		   		re.put("ldate", new ObjectByteIterator(String.valueOf(v.getProperty("ldate")).getBytes()));
		   		re.put("address", new ObjectByteIterator(String.valueOf(v.getProperty("address")).getBytes()));
		   		re.put("email", new ObjectByteIterator(String.valueOf(v.getProperty("email")).getBytes()));
		   		re.put("tel", new ObjectByteIterator(String.valueOf(v.getProperty("tel")).getBytes()));
		   		
		   		result.add(re);
		   		
		   		tx1.commit();
		       	//System.out.println(edge.getVertex(Direction.IN).getId());
		       }
		
		
		return SUCCESS;
	
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		if(profileOwnerID<0)
		{
			return ERROR;
		}
		
		// get the vertex from profile owner id
		String requester_id = Integer.toString(profileOwnerID);
		Iterable it = g.query().has("userid",Compare.EQUAL,requester_id).vertices();
		Vertex owner = (Vertex) it.iterator().next();
		 
	       	for(Edge edge1 : g.getVertex(owner.getId()).getEdges(Direction.OUT, "pending"))
		       {
		       	tx1=g.newTransaction();
		       	Vertex v1=edge1.getVertex(Direction.IN);
		       	int friendcnt=0;
		   		Iterable friend_cnt = v1.query().has("user_relationship",Text.CONTAINS,"friendship").edges();
		   		if(Iterables.size(friend_cnt)!=0)
		   			friendcnt=Iterables.size(friend_cnt);

		   		
		   		//Get create count
		   		int resourcecnt =0;
		   		Iterable resource_cnt = v1.query().has("src_relationship",Text.CONTAINS,"create").edges();
		   		if(Iterables.size(resource_cnt)!=0)
		   			resourcecnt = Iterables.size(resource_cnt);
		   		
		   		
		   		HashMap <String, ByteIterator>re = new HashMap<String, ByteIterator>();
		   		
		   		
		   		re.put("friendcount", new ObjectByteIterator(Integer.toString(friendcnt).getBytes()));
		   		re.put("resourcecount", new ObjectByteIterator(Integer.toString(resourcecnt).getBytes()));
		   		re.put("username", new ObjectByteIterator(String.valueOf(v1.getProperty("username")).getBytes()));
		   		re.put("fname", new ObjectByteIterator(String.valueOf(v1.getProperty("fname")).getBytes()));
		   		re.put("gender", new ObjectByteIterator(String.valueOf(v1.getProperty("gender")).getBytes()));
		   		re.put("dob", new ObjectByteIterator(String.valueOf(v1.getProperty("dob")).getBytes()));
		   		re.put("jdate", new ObjectByteIterator(String.valueOf(v1.getProperty("jdate")).getBytes()));
		   		re.put("ldate", new ObjectByteIterator(String.valueOf(v1.getProperty("ldate")).getBytes()));
		   		re.put("address", new ObjectByteIterator(String.valueOf(v1.getProperty("address")).getBytes()));
		   		re.put("email", new ObjectByteIterator(String.valueOf(v1.getProperty("email")).getBytes()));
		   		re.put("tel", new ObjectByteIterator(String.valueOf(v1.getProperty("tel")).getBytes()));
		   		
		   		results.add(re);
		   		
		   		tx1.commit();
		       	//System.out.println(edge.getVertex(Direction.IN).getId());
		       }
	       	//System.out.println(edge.getVertex(Direction.IN).getId());
	       
		return SUCCESS;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		
		// remove pending link and change it to accept
	
		int retVal = SUCCESS;

		if (inviterID < 0 || inviteeID < 0)
			return ERROR;

		int res1 = deletePendingRequest(inviterID, inviteeID); //remove pending link
		int res2 = CreateFriendship(inviterID, inviteeID); //call createfriendship to create a friend link

		if ((ERROR == res1) || (ERROR == res2))
			return ERROR;

		return retVal;
	}
	
	
	
	
	public int deletePendingRequest(int inviterID, int inviteeID) {

		int retVal = SUCCESS;


		if (inviterID < 0 || inviteeID < 0)
			return ERROR;
		
		TitanTransaction tx = g.newTransaction();
		String frnd1 = Integer.toString(inviterID);
		String frnd2 = Integer.toString(inviteeID);
		tx1 = g.newTransaction();
		Iterable it = g.query().has("userid",Compare.EQUAL,frnd1).vertices();
		Vertex v1 = (Vertex) it.iterator().next();
		Iterable it1 = g.query().has("userid",Compare.EQUAL,frnd2).vertices();
		Vertex v2 = (Vertex) it1.iterator().next();
		
		
		List<Edge> edges = new ArrayList<Edge>();
		for(Edge edge : g.getVertex(v1.getId()).getEdges(Direction.OUT, "pending"))
		{
		   if(edge.getVertex(Direction.IN).getId().equals(v2.getId()))
				   {
			   			g.removeEdge(edge);
			   			break;
			   		}
		}

		tx.commit();
		return retVal;
	}

	
	

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		return deletePendingRequest(inviterID, inviteeID);
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		
		
		int retVal = SUCCESS;
		if (inviterID < 0 || inviteeID<0)
		{
			return ERROR;
		}
		
		String frnd1 = Integer.toString(inviterID);
		String frnd2 = Integer.toString(inviteeID);
		tx1 = g.newTransaction();
		Iterable it = g.query().has("userid",Compare.EQUAL,frnd1).vertices();
		Vertex v1 = (Vertex) it.iterator().next();
		Iterable it1 = g.query().has("userid",Compare.EQUAL,frnd2).vertices();
		Vertex v2 = (Vertex) it1.iterator().next();
		v1.addEdge("pending", v2);
		tx1.commit();
		
		
		return retVal;
			
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		if(requesterID<0||profileOwnerID<0)
		{
			return ERROR;
		}
		
		int count=0;		
		
		String owner = Integer.toString(profileOwnerID);
		//String frnd2 = Integer.toString(inviteeID);
		tx1 = g.newTransaction();
		Iterable it = g.query().has("userid",Compare.EQUAL,owner).vertices();
		Vertex v1 = (Vertex) it.iterator().next();
		
		for(Edge edge : g.getVertex(v1.getId()).getEdges(Direction.OUT, "create"))
		{
			
			HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
			
			if(count==k)
			{
				break;
			}
			count++;
			Vertex res=edge.getVertex(Direction.IN);
			
			values.put("rid", new ObjectByteIterator(String.valueOf(res.getProperty("rid")).getBytes()));
			values.put("creatorid", new ObjectByteIterator(String.valueOf(res.getProperty("creatorid")).getBytes()));
			values.put("walluserid", new ObjectByteIterator(String.valueOf(res.getProperty("walluserid")).getBytes()));
			values.put("type", new ObjectByteIterator(String.valueOf(res.getProperty("type")).getBytes()));
			values.put("body", new ObjectByteIterator(String.valueOf(res.getProperty("body")).getBytes()));
			values.put("doc", new ObjectByteIterator(String.valueOf(res.getProperty("doc")).getBytes()));

			
			result.add(values);    

		}
		
		return SUCCESS;
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		if(requesterID<0||profileOwnerID<0||resourceID<0)
		{
			return ERROR;
		}
		
		
		tx1=g.newTransaction();
		String resource_id = Integer.toString(resourceID);
		Iterable it1 = g.query().has("rid",Compare.EQUAL,resource_id).vertices();
		Vertex resource = (Vertex) it1.iterator().next();
		
		   for(Edge edge : g.getVertex(resource.getId()).getEdges(Direction.OUT, "manipulation"))
	       {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

	       	Vertex mani=edge.getVertex(Direction.IN);
	       	values.put("timestamp", new ObjectByteIterator(String.valueOf(mani.getProperty("timestamp")).getBytes()));
	       	values.put("type", new ObjectByteIterator(String.valueOf(mani.getProperty("type")).getBytes()));
			values.put("content", new ObjectByteIterator(String.valueOf(mani.getProperty("content")).getBytes()));
			values.put("mid", new ObjectByteIterator(((String) mani.getProperty("mid")).getBytes())); 
			values.put("modifierid", new ObjectByteIterator(((String) mani.getProperty("modifierid")).getBytes()));
			values.put("creatorid", new ObjectByteIterator(((String) mani.getProperty("creatorid")).getBytes()));
			result.add(values);
	       }
		
		tx1.commit();
		
		return SUCCESS;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID,
			int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		if(commentCreatorID<0||resourceCreatorID<0||resourceID<0)
		{
			return ERROR;
		}
		tx1 = g.newTransaction();

		Vertex mani_ver = tx1.addVertexWithLabel("manipulate");
		
		String check = null;
		for (Entry<String, ByteIterator> entry : values.entrySet()) {						
			
			if(entry.getKey().equals("mid")){
				check = entry.getValue().toString();
				
			}
			
			String type = null;
			
			if(entry.getKey().equals("type")){
				type = entry.getValue().toString();
				mani_ver.setProperty(entry.getKey(), type);
			}
			else
			{
				mani_ver.setProperty(entry.getKey(), entry.getValue().toString());
			}
		}
		
		
		
		mani_ver.setProperty("creatorid", Integer.toString(resourceCreatorID));
		mani_ver.setProperty("rid", Integer.toString(resourceID));
		mani_ver.setProperty("modifierid", Integer.toString(commentCreatorID));
	
		tx1.commit();
			
		
		tx1 = g.newTransaction();
		
		Iterable it2 = g.query().has("mid",Compare.EQUAL,check).vertices();
		mani_ver = (Vertex) it2.iterator().next();

		String commentcreater_id = Integer.toString(commentCreatorID);
		Iterable it = g.query().has("userid",Compare.EQUAL,commentcreater_id).vertices();
		Vertex creater = (Vertex) it.iterator().next();
		
		creater.addEdge("posts", mani_ver);
	
		String resourceid = Integer.toString(resourceID);
		Iterable it1 = g.query().has("rid",Compare.EQUAL,resourceid).vertices();
		Vertex resource = (Vertex) it1.iterator().next();
		
		resource.addEdge("manipulation", mani_ver).setProperty("rsc", "manipulation");
		
		tx1.commit();
		
		tx1 = g.newTransaction();		
		
		String resourceid1 = Integer.toString(resourceID);
		Iterable it7 = g.query().has("rid",Compare.EQUAL,resourceid1).vertices();
		Vertex resource1 = (Vertex) it7.iterator().next();
		
		   for(Edge edge : g.getVertex(resource1.getId()).getEdges(Direction.OUT, "manipulation"))
	       {
			   Vertex v=edge.getVertex(Direction.IN);
	       }
		
		tx1.commit();
	
		return SUCCESS;
	}

	

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub
		
		if(resourceCreatorID<0||resourceID<0||manipulationID<0)
		{
			return ERROR;
		}
		
		
		tx1=g.newTransaction();
		String resource_id = Integer.toString(resourceID);
		Iterable it1 = g.query().has("rid",Compare.EQUAL,resource_id).vertices();
		Vertex resource = (Vertex) it1.iterator().next();
		
		
		String manipulation_id = Integer.toString(manipulationID);
		Iterable it2 = g.query().has("mid",Compare.EQUAL,manipulation_id).vertices();
		Vertex mani = (Vertex) it2.iterator().next();
		
		   for(Edge edge : g.getVertex(resource.getId()).getEdges(Direction.OUT, "manipulation"))
	       {
			   if(edge.getVertex(Direction.IN).getId().equals(mani.getId()))
			   {
				    Vertex v=edge.getVertex(Direction.IN);
		   			g.removeVertex(v);
		   			
		   			break;	
		   		}
	       }	       
		
		tx1.commit();
		
		return SUCCESS;

	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		
		int retVal = SUCCESS;
		if (friendid1 < 0 || friendid2<0)
		{
			return ERROR;
		}
		
		
		
		String frnd1 = Integer.toString(friendid1);
		String frnd2 = Integer.toString(friendid2);
		tx1 = g.newTransaction();
		Iterable it = g.query().has("userid",Compare.EQUAL,frnd1).vertices();
		Vertex v1 = (Vertex) it.iterator().next();
		Iterable it1 = g.query().has("userid",Compare.EQUAL,frnd2).vertices();
		Vertex v2 = (Vertex) it1.iterator().next();

		
		/*Iterable v_edges = v1.query().has("user_relationship",Text.CONTAINS,"friend").edges();
		Edge rem_edge = (Edge) v_edges.iterator().next();
		g.removeEdge(rem_edge);*/
		
		int check=0;

		for(Edge edge : g.getVertex(v1.getId()).getEdges(Direction.OUT, "friendship"))
		{
		   if(edge.getVertex(Direction.IN).getId().equals(v2.getId()))
				   {
			   			g.removeEdge(edge);
			   			check=1;
			   			break;
			   		}
		}
		
		if(check != 1){
		for(Edge edge : g.getVertex(v2.getId()).getEdges(Direction.OUT, "friendship"))
		{
		   if(edge.getVertex(Direction.IN).getId().equals(v1.getId()))
				   {
			   			g.removeEdge(edge);
			   			break;
			   		}
		}
		}
		
		
		tx1.commit();
		
		
		return retVal;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> hm = new HashMap<String, String>();
	     
		hm.put("usercount", "100");
		hm.put("resourcesperuser", "10");
		hm.put("avgfriendsperuser", "10");
		hm.put("avgpendingperuser", "0");
		
		return hm;
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		//System.out.println("Create Frnd");
		// TODO Auto-generated method stub
		int retVal = SUCCESS;
		
		if(retVal<0)
		{
			return ERROR;
		}
		
		String frnd1 = Integer.toString(friendid1);
		String frnd2 = Integer.toString(friendid2);
		tx1 = g.newTransaction();
		Iterable it = g.query().has("userid",Compare.EQUAL,frnd1).vertices();
		Vertex v1 = (Vertex) it.iterator().next();
		Iterable it1 = g.query().has("userid",Compare.EQUAL,frnd2).vertices();
		Vertex v2 = (Vertex) it1.iterator().next();
		v1.addEdge("friendship", v2);
		tx1.commit();
		return retVal;
	
	}

	@Override
	public void createSchema(Properties props) {

	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		// TODO Auto-generated method stub
		return 0;
	}

}

