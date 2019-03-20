

import java.util.ArrayList;

import org.eclipse.rdf4j.repository.sail.SailRepository;

public class RDF4JTestDriver {
	
		
	public static ArrayList<String> runTest(REPO_TYPES repoType, String sourceRDFFile, String dataDirPath, int queryIterations)
	{		
		ArrayList<String> results = new ArrayList<String>();
		results.add("Repository Type = " + repoType.toString());
		SailRepository repo = RDF4JUtils.createRepository(repoType,dataDirPath);
		results.add("Load Time : " + RDF4JUtils.loadData(repo, sourceRDFFile));
		
		SPARQLQuerySet mySet = new SPARQLQuerySet();
		long  runTimes[][] = new long[mySet.getQueryCount()][queryIterations];
		long  runResultCount[] = new long[mySet.getQueryCount()];
				
		for (int r=1;r<=queryIterations;r++) {
			System.out.println();			
			System.out.println(" ++ iteration No " + r );
			for(int i=0;i<mySet.getQueryCount();i++)
			{
				SPARQLQuerySetMember queryItem = mySet.getItem(i);
				//runTimes[i][r-1] = RDF4JUtils.runSPARQL(repo, queryItem.getQueryName(), queryItem.getQueryString());	
				System.out.println("     " + queryItem.getQueryName());
				if(r==1) {
					Pair p = RDF4JUtils.runSPARQL(repo, queryItem.getQueryName(), queryItem.getQueryString(),true,dataDirPath);	
					runTimes[i][r-1] = p.getTime();
					runResultCount[i] = p.getCount();
					}
				else {
					Pair p = RDF4JUtils.runSPARQL(repo, queryItem.getQueryName(), queryItem.getQueryString(),false,null);	
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
		/*
		results.add(" --- Query Result Count --- ");
		for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			String strResult = queryItem.getQueryName() + " :: ";					
			strResult = strResult + runResultCount[i] ;
			results.add(strResult);
		}*/
		
		repo.shutDown();
		repo = null;
		return(results);
	}


}
