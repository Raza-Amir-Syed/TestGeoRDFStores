import java.util.ArrayList;


import com.complexible.stardog.Stardog;
import com.complexible.stardog.sesame.StardogRepository;


public class StardogTestDriver {
	
	public static ArrayList<String> runTest(String dbName, String sourceRDFFile, int queryIterations, String storagePath)
	{		
		ArrayList<String> results = new ArrayList<String>();
				
		results.add("++++Stardog SNARL Results++++");
					
		SPARQLQuerySet mySet = new SPARQLQuerySet();
		/*long  runTimes[][] = new long[mySet.getQueryCount()][queryIterations];
		
		for (int r=1;r<=queryIterations;r++) {
			System.out.println("-----------------------------------iter = " + r);
			for(int i=0;i<mySet.getQueryCount();i++)
			{
				SPARQLQuerySetMember queryItem = mySet.getItem(i);
				System.out.println(queryItem.getQueryName());
				runTimes[i][r-1] = StardogUtils.runSPARQL(dbName, queryItem.getQueryName(), queryItem.getQueryString());
			}
			
		}
		
		for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			String strResult = queryItem.getQueryName() + " :: ";
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runTimes[i][j] + ",";
			}
			results.add(strResult);
		}*/
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
					Pair p = StardogUtils.runSPARQL(dbName, queryItem.getQueryName(), queryItem.getQueryString(),true,storagePath);
					runTimes[i][r-1] = p.getTime();
					runResultCount[i] = p.getCount();
					}
				else {
					Pair p = StardogUtils.runSPARQL(dbName, queryItem.getQueryName(), queryItem.getQueryString(),false,null);
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
		
		return(results);
	}
	
	public static ArrayList<String> runRDF4JTest(String dbName, String sourceRDFFile, int queryIterations, String storagePath)
	{		
		ArrayList<String> results = new ArrayList<String>();
				
		results.add("++++Stardog Instance RDF4J Results++++");
		StardogRepository repo = StardogUtils.createRepository(dbName);
						
		SPARQLQuerySet mySet = new SPARQLQuerySet();
		/*long  runTimes[][] = new long[mySet.getQueryCount()][queryIterations];
		
		for (int r=1;r<=queryIterations;r++) {		
			System.out.println("-----------------------------------iter = " + r);
			for(int i=0;i<mySet.getQueryCount();i++)
			{
				SPARQLQuerySetMember queryItem = mySet.getItem(i);
				System.out.println(queryItem.getQueryName());
				runTimes[i][r-1] = StardogUtils.runSPARQLOnRepo(repo, queryItem.getQueryName(), queryItem.getQueryString());
			}
			
		}
		
		for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			String strResult = queryItem.getQueryName() + " :: ";
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runTimes[i][j] + ",";
			}
			results.add(strResult);
		}*/
		
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
					Pair p = StardogUtils.runSPARQLOnRepo(repo, queryItem.getQueryName(), queryItem.getQueryString(),true,storagePath);
					runTimes[i][r-1] = p.getTime();
					runResultCount[i] = p.getCount();
					}
				else {
					Pair p = StardogUtils.runSPARQLOnRepo(repo, queryItem.getQueryName(), queryItem.getQueryString(),false,null);
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
	
		repo.shutDown();
		return(results);
	}

}
