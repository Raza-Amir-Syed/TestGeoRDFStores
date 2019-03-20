import java.sql.Statement;
import java.util.ArrayList;

import org.eclipse.rdf4j.repository.sail.SailRepository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import virtuoso.rdf4j.driver.VirtuosoRepository;

public class VirtuosoTestDriver {
	
	public static  ArrayList<String> runVirtuosoJDBCTest(String host,String port,String username,String pwd,int queryIterations)
	{
		ArrayList<String> results = new ArrayList<String>();	
		Connection conn =  VirtuosoUtils.getJDBCconnection(host,port,username,pwd);
		
		results.add(" +++++ Virtuoso JDBC Tests +++++ "); 
		SPARQLQuerySet mySet = new SPARQLQuerySet();
		
		long  runTimes[][] = new long[mySet.getQueryCount()][queryIterations];
		long  runResultCount[] = new long[mySet.getQueryCount()];
				
		for (int r=1;r<=queryIterations;r++) {
						
			System.out.println();			
			System.out.println(" ++ iteration No " + r );
			for(int i=0;i<mySet.getQueryCount();i++)
			{
				SPARQLQuerySetMember queryItem = mySet.getItem(i);
				System.out.println("     " + queryItem.getQueryName());
				
				if(r==1) {
					Pair p = VirtuosoUtils.runSPARQLJDBC(conn, queryItem.getQueryName(), queryItem.getQueryString(),true);
					runTimes[i][r-1] = p.getTime();
					runResultCount[i] = p.getCount();
					}
				else {
					Pair p = VirtuosoUtils.runSPARQLJDBC(conn, queryItem.getQueryName(), queryItem.getQueryString(),false);
					runTimes[i][r-1] = p.getTime();					
				}
			}			
		}
		
		for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			
			String strResult = "";
			double sumMilliSecAfter20Iterations = 0;
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runTimes[i][j] + ",";
				if(j>19) {
					sumMilliSecAfter20Iterations += (runTimes[i][j]/1000000.0);
				}
			}		
			if(queryIterations>20) {
				double avgMilliSecAfter20Iterations = sumMilliSecAfter20Iterations/(queryIterations-20);
				strResult = queryItem.getQueryName() + " :: Count : " + runResultCount[i] + 
						    ", Average excluding first 20 (MS) : " + avgMilliSecAfter20Iterations + ", Timings (NS) : " + "" + strResult;
			}
			else{
				strResult = queryItem.getQueryName() + " :: Count = " + runResultCount[i] + " Timings (NS) : " + "" + strResult;	
			}
			results.add(strResult);
		}
		
		/*long  runTimes[][] = new long[mySet.getQueryCount()][queryIterations];
		long  runResultCount[][] = new long[mySet.getQueryCount()][queryIterations];
				
		for (int r=1;r<=queryIterations;r++) {	
			System.out.println("run " + r);
			for(int i=0;i<mySet.getQueryCount();i++)
			{
				SPARQLQuerySetMember queryItem = mySet.getItem(i);						
				Pair p = VirtuosoUtils.runSPARQLJDBC(conn, queryItem.getQueryName(), queryItem.getQueryString());	
				runTimes[i][r-1] = p.getTime();
				runResultCount[i][r-1] = p.getCount();
			}		
		}
		
		results.add(" + Running the Virtuoso JDBC Tests ");
		
		for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			String strResult = queryItem.getQueryName() + " :: ";
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runTimes[i][j] + ",";
			}
			strResult = strResult + " // " ;
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runResultCount[i][j] + "--";
			}
			results.add(strResult);
		}	
		
		try {
			
			conn.close();
			conn=null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		return(results);
		
	}
	
	public static ArrayList<String>  runVirtuosoRDF4JTest(String host,String port,String username,String pwd,String RDFFileName, int queryIterations)
	{
		
		ArrayList<String> results = new ArrayList<String>();	
		
		VirtuosoRepository repo =  VirtuosoUtils.createRepository(host,port,username,pwd);
		results.add("Load Time : " + VirtuosoUtils.loadData(repo, RDFFileName));
		results.add(" +++++ Virtuoso RDF4J Tests +++++ "); 
		SPARQLQuerySet mySet = new SPARQLQuerySet();
		
		long  runTimes[][] = new long[mySet.getQueryCount()][queryIterations];
		long  runResultCount[] = new long[mySet.getQueryCount()];
				
		for (int r=1;r<=queryIterations;r++) {
						
			System.out.println();			
			System.out.println(" ++ iteration No " + r );
			for(int i=0;i<mySet.getQueryCount();i++)
			{
				SPARQLQuerySetMember queryItem = mySet.getItem(i);
				System.out.println("     " + queryItem.getQueryName());
				
				if(r==1) {
					Pair p = VirtuosoUtils.runSPARQL(repo, queryItem.getQueryName(), queryItem.getQueryString(),true);	
					runTimes[i][r-1] = p.getTime();
					runResultCount[i] = p.getCount();
					}
				else {
					Pair p = VirtuosoUtils.runSPARQL(repo, queryItem.getQueryName(), queryItem.getQueryString(),false);	
					runTimes[i][r-1] = p.getTime();					
				}
			}			
		}
		
		for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			
			String strResult = "";
			double sumMilliSecAfter20Iterations = 0;
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runTimes[i][j] + ",";
				if(j>19) {
					sumMilliSecAfter20Iterations += (runTimes[i][j]/1000000.0);
				}
			}		
			if(queryIterations>20) {
				double avgMilliSecAfter20Iterations = sumMilliSecAfter20Iterations/(queryIterations-20);
				strResult = queryItem.getQueryName() + " :: Count : " + runResultCount[i] + 
						    ", Average excluding first 20 (MS) : " + avgMilliSecAfter20Iterations + ", Timings (NS) : " + "" + strResult;
			}
			else{
				strResult = queryItem.getQueryName() + " :: Count = " + runResultCount[i] + " Timings (NS) : " + "" + strResult;	
			}
			results.add(strResult);
		}
		
		/*long  runTimes[][] = new long[mySet.getQueryCount()][queryIterations];
		long  runResultCount[][] = new long[mySet.getQueryCount()][queryIterations];
				
		for (int r=1;r<=queryIterations;r++) {			
			System.out.println("run " + r);
			for(int i=0;i<mySet.getQueryCount();i++)
			{
				SPARQLQuerySetMember queryItem = mySet.getItem(i);						
				Pair p = VirtuosoUtils.runSPARQL(repo, queryItem.getQueryName(), queryItem.getQueryString());	
				runTimes[i][r-1] = p.getTime();
				runResultCount[i][r-1] = p.getCount();
			}		
		}
		
		results.add(" + Running the Virtuoso RDF4J Tests "); 
		
		for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			String strResult = queryItem.getQueryName() + " :: ";
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runTimes[i][j] + ",";
			}
			strResult = strResult + " // " ;
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runResultCount[i][j] + "--";
			}
			results.add(strResult);
		}	*/
		
				
		repo.shutDown();
		repo = null;
		return(results);
		
	
	}

}
