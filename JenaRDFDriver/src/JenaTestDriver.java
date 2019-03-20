
import org.slf4j.Logger ;



import org.slf4j.LoggerFactory ;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import javax.management.Query;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.spatial.EntityDefinition;
import org.apache.jena.query.spatial.SpatialDatasetFactory;
import org.apache.jena.query.spatial.SpatialIndex;
import org.apache.jena.query.spatial.SpatialIndexLucene;
import org.apache.jena.query.spatial.SpatialQuery;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.util.QueryExecUtils;
import org.apache.jena.tdb.TDBFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class JenaTestDriver {
	   
    public static ArrayList<String> runTest(DATASET_TYPES setType, String sourceRDFFile, String dataDirPath, int queryIterations) throws IOException
    {
    	ArrayList<String> results = new ArrayList<String>();
    	Dataset spatialDataset=null;
    	File indexDirectory = new File(dataDirPath + "/indexes");
    	File TDBDataDir = new File(dataDirPath);
    	if(setType == DATASET_TYPES.MEMORY) {
    		spatialDataset = JenaUtils.initInMemoryDatasetWithLuceneSpatialIndex(indexDirectory);
    	}else if(setType == DATASET_TYPES.TDB) {
    		spatialDataset = JenaUtils.initTDBDatasetWithLuceneSpatialIndex(indexDirectory, TDBDataDir);
    	}
    	results.add("Data Set Type = " + setType.toString());
    	results.add("Load Time : " + JenaUtils.loadData(spatialDataset, sourceRDFFile));
    	
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
					Pair p = JenaUtils.runSPARQL(spatialDataset, queryItem.getQueryName(), queryItem.getQueryString(),true,dataDirPath);	
					runTimes[i][r-1] = p.getTime();
					runResultCount[i] = p.getCount();
					}
				else {
					Pair p = JenaUtils.runSPARQL(spatialDataset, queryItem.getQueryName(), queryItem.getQueryString(),false,null);	
					runTimes[i][r-1] = p.getTime();					
				}
				
				/*SPARQLQuerySetMember queryItem = mySet.getItem(i);
				runTimes[i][r-1] = JenaUtils.runSPARQL(spatialDataset, queryItem.getQueryName(), queryItem.getQueryString());	*/
			}			
		}
		
		/*for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			String strResult = queryItem.getQueryName() + " :: ";
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runTimes[i][j] + ",";
			}
			results.add(strResult);
		}*/
		/*for(int i=0;i<mySet.getQueryCount();i++) {
			SPARQLQuerySetMember queryItem = mySet.getItem(i);
			String strResult = queryItem.getQueryName() + " :: ";
			for(int j=0;j<queryIterations;j++) {
				strResult = strResult + runTimes[i][j] + ",";
			}		
			strResult = strResult + " // " ;
			strResult = strResult + runResultCount[i] ;
			results.add(strResult);
		}*/
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
		
		
		
		
		
		//JenaUtils.destroy(spatialDataset);
    	spatialDataset.close();
    	spatialDataset = null;    	
    	return(results);
    }
  
    
    

}
