
import java.io.BufferedWriter;
import java.io.File;



import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
//import org.eclipse.rdf4j.query.resultio.QueryResultIO;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.UnsupportedQueryResultFormatException;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryResult;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.resultio.QueryResultFormat;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultWriterFactory;
import org.openrdf.rio.RDFFormat;
import org.eclipse.rdf4j.repository.Repository;

/*import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.rio.RDFFormat;*/

import com.complexible.common.openrdf.repository.RepositoryConnections;
import com.complexible.common.rdf.query.TupleQueryResultIterator;
import com.complexible.common.rdf.query.resultio.TextTableQueryResultWriter;
import com.complexible.stardog.Stardog;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.SelectQuery;
import com.complexible.stardog.api.admin.AdminConnection;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.complexible.stardog.sesame.StardogRepository;
import com.complexible.stardog.sesame.StardogRepositoryConnection;
import com.complexible.stardog.spatial.GeospatialOptions;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.query.SelectQueryResult;
import com.stardog.stark.query.io.QueryResultWriters;
import com.stardog.stark.query.io.ResultWritingFailed;


public class StardogUtils {

	public static Stardog startServerInstance() {
		Stardog stardogInstance = Stardog.builder().set(GeospatialOptions.USE_JTS,true).create();
		return(stardogInstance);
	}
	public static void createDatabase(String dbName) {
		AdminConnection stardogAdminConnection = AdminConnectionConfiguration.toEmbeddedServer().credentials("admin", "admin").connect();
		if (stardogAdminConnection.list().contains(dbName)) {
			stardogAdminConnection.drop(dbName);
		}
		stardogAdminConnection.newDatabase(dbName).set(GeospatialOptions.SPATIAL_ENABLED, true).create();
		stardogAdminConnection.close();
	}
	
	public static long loadData(String dbName, String fileName) {
		
		long startTime=0;
		long stopTime=0;
		long elapsedTime=0;
		startTime = System.currentTimeMillis();
		Connection stardogConnection = ConnectionConfiguration.to(dbName).credentials("admin", "admin").connect();
		stardogConnection.begin();

		try {
			stardogConnection.add().io().format(RDFFormats.RDFXML).stream(new FileInputStream(fileName));			
		} catch (StardogException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		stardogConnection.commit();			
		stardogConnection.close();
		stopTime = System.currentTimeMillis();
		elapsedTime = stopTime-startTime;
		
		return(elapsedTime);
	}
	
	public static long loadDataToRepo(String fileName,StardogRepository repo) {
		
		long startTime,stopTime,elapsedTime=0;
		startTime = System.currentTimeMillis();
		
//		File file1 = new File(fileName);				
//		String baseURI = "http://meta.icos-cp.eu/resources/socat/";
		StardogRepositoryConnection conn = repo.getConnection();
		try{
			RepositoryConnections.add(conn, new FileInputStream(fileName), RDFFormat.RDFXML);			
			stopTime = System.currentTimeMillis();
			elapsedTime = stopTime-startTime;			
		}
		catch (RDF4JException e) {
			System.out.println("RDF4 Exception in Data Load");
		}
		catch (IOException e) {
			System.out.println("I/O Exception in Data Load");
		}
		
		conn.close();
		return(elapsedTime);
	}
	public static Pair runSPARQL(String dbName, String qryName,String qryString, boolean queryCount,String dirPath) {
		long startTime=0;
		long stopTime=0;
		long elapsedTime=0;
		long cnt=0;
		Connection stardogConnection = ConnectionConfiguration.to(dbName).credentials("admin", "admin").connect();
		try {
			String queryString = qryString;
			//startTime = System.currentTimeMillis();
			startTime = System.nanoTime();
			SelectQuery SPARQLQuery = stardogConnection.select(qryString);	
			SelectQueryResult ResultSet = SPARQLQuery.execute();
			ResultSet.hasNext();
			//stopTime = System.currentTimeMillis();
			stopTime = System.nanoTime();
			elapsedTime = stopTime-startTime;	
			if(queryCount) {
				cnt = getResultCount(ResultSet,qryName,qryString,dirPath);
			}
			//QueryResultWriters.write(ResultSet, System.out, TextTableQueryResultWriter.FORMAT);			
		} catch (ResultWritingFailed  e) {
			e.printStackTrace();
		}	
		finally { 
			stardogConnection.close();		
		}
		return(new Pair(elapsedTime,cnt));
	}
	
	private static long getResultCount(SelectQueryResult tupleQueryResult,String qryName,String qryString, String dirPath)
	{
		long cnt =0;
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(dirPath + "/StardogResults.txt", true)); 
	        out.write("\n ++ Query (" + qryName + ") :: \n");  
			while (tupleQueryResult.hasNext()) {
				cnt++;
				com.stardog.stark.query.BindingSet bset = tupleQueryResult.next();
				out.write(bset.get("result").toString());
				out.write("\n"); 				
			}
			out.close();
		}catch (IOException e) { 
            System.out.println("exception occoured" + e); 
        }
		return(cnt);				
	}
	
	public static StardogRepository createRepository(String dbName) {
		
		StardogRepository repo =  new StardogRepository(ConnectionConfiguration.to(dbName).credentials("admin", "admin")) ;
		repo.initialize();		
		return(repo);		
	}
	public static Pair runSPARQLOnRepo(StardogRepository repo, String qryName,String qryString, boolean queryCount,String dirPath) {
		long startTime=0;
		long stopTime=0;
		long elapsedTime=0;
		long cnt=0;
		//StardogRepositoryConnection conn =  StardogRepo.getConnection();
		StardogRepositoryConnection conn = repo.getConnection();
		try{
			//startTime = System.currentTimeMillis();
			startTime = System.nanoTime();
			
			org.openrdf.query.TupleQuery SPARQLQuery = conn.prepareTupleQuery(qryString);
			QueryResult<BindingSet> ResultSet = SPARQLQuery.evaluate();
			ResultSet.hasNext();
			//stopTime = System.currentTimeMillis();
			stopTime = System.nanoTime();
			elapsedTime = stopTime-startTime;	
			if(queryCount) {
				cnt = getResultCount(ResultSet,qryName,qryString,dirPath);
			}	
			/*System.out.println("Query (" + qryName + ")");
			while (ResultSet.hasNext()) { 						
				BindingSet bindingSet = ResultSet.next();
				org.openrdf.model.Value resultObject = bindingSet.getValue("geom1");							
				System.out.println("  " + resultObject.toString());
			}*/
				
		}	 catch (ResultWritingFailed e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TupleQueryResultHandlerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedQueryResultFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  finally { 
				conn.close();
			
		}
			
		return(new Pair(elapsedTime,cnt));
	}
	private static long getResultCount(QueryResult<BindingSet> tupleQueryResult,String qryName,String qryString, String dirPath)
	{
		/*long cnt =0;
		while (tupleQueryResult.hasNext()) {
			cnt++;
			tupleQueryResult.next();
		}
		return(cnt);	*/		
		
		long cnt =0;
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(dirPath + "/StardogResults.txt", true)); 
	        out.write("\n ++ Query (" + qryName + ") :: \n");  
			while (tupleQueryResult.hasNext()) {
				cnt++;
				BindingSet bindingSet = tupleQueryResult.next();
				org.openrdf.model.Value resultObject = bindingSet.getValue("result");
				out.write(resultObject.toString());
				out.write("\n"); 				
			}
			out.close();
		}catch (IOException e) { 
            System.out.println("exception occoured" + e); 
        }
		return(cnt);
	}
	
}
