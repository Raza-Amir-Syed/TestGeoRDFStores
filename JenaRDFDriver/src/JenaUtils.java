import java.io.BufferedWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.spatial.EntityDefinition;
import org.apache.jena.query.spatial.SpatialDatasetFactory;
import org.apache.jena.query.spatial.SpatialIndex;
import org.apache.jena.query.spatial.SpatialIndexLucene;
import org.apache.jena.query.spatial.SpatialQuery;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.sparql.util.QueryExecUtils;
import org.apache.jena.tdb.TDBFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


public class JenaUtils {
	
	private static File indexDir=null;
	private static File tdbDir=null;
	
	public static Dataset initInMemoryDatasetWithLuceneSpatialIndex(File indexDir) throws IOException
    {
		JenaUtils.indexDir = indexDir;	
		 deleteOldFiles(JenaUtils.indexDir);
		JenaUtils.indexDir.mkdirs();
        return createDatasetByCode(indexDir);
    }
    
    public static Dataset initTDBDatasetWithLuceneSpatialIndex(File indexDir, File TDBDir) throws IOException{
    	JenaUtils.indexDir = indexDir;	
    	JenaUtils.tdbDir = TDBDir;
    	deleteOldFiles(JenaUtils.indexDir);
	    deleteOldFiles(JenaUtils.tdbDir);	     
    	JenaUtils.indexDir.mkdirs();
    	JenaUtils.tdbDir.mkdir();
        return createDatasetByCode(indexDir, TDBDir);
    }
    
    private static Dataset createDatasetByCode(File indexDir) throws IOException {
        // Base data
        Dataset ds1 = DatasetFactory.create();
        return joinDataset(ds1, indexDir);
    }
    private static Dataset createDatasetByCode(File indexDir, File TDBDir) throws IOException {
        // Base data
        Dataset ds1 = TDBFactory.createDataset(TDBDir.getAbsolutePath());
        return joinDataset(ds1, indexDir);
    }
    
    private static Dataset joinDataset(Dataset baseDataset, File indexDir) throws IOException{
        EntityDefinition entDef = new EntityDefinition("entityField", "geoField");        
        entDef.setSpatialContextFactory(SpatialQuery.JTS_SPATIAL_CONTEXT_FACTORY_CLASS);
        Directory dir = FSDirectory.open(indexDir.toPath());
        Dataset ds = SpatialDatasetFactory.createLucene(baseDataset, dir, entDef);
        return ds;
    }
    
    public static long loadData(Dataset spatialDataset, String file) {
        
        long startTime,stopTime,elapsedTime = 0;
        startTime = System.currentTimeMillis();
        spatialDataset.begin(ReadWrite.WRITE);
        try {
            Model m = spatialDataset.getDefaultModel();
            m.read(new FileInputStream(file),null);            
            spatialDataset.commit();
        } catch (IOException e) {
			e.printStackTrace();
		} finally {
            spatialDataset.end();
        }

        stopTime = System.currentTimeMillis();
        elapsedTime = (stopTime - startTime);;
        return(elapsedTime);
    }
    
    public static Pair runSPARQL(Dataset spatialDataset,String qryName,String qryString,boolean queryCount,String dirPath) {
        long startTime=0;
        long stopTime=0;
        long elapsedTime=0;
        long countResult=0;	
        //startTime = System.currentTimeMillis();
        startTime = System.nanoTime();
	    spatialDataset.begin(ReadWrite.READ);
	    try {
	            org.apache.jena.query.Query q = QueryFactory.create(qryString);	            
	            QueryExecution qexec = QueryExecutionFactory.create(q, spatialDataset);	            
	            ResultSet results = qexec.execSelect() ;
	            results.hasNext();
	            //stopTime = System.currentTimeMillis();
	            stopTime = System.nanoTime();
	            elapsedTime = stopTime - startTime ;
	            if(queryCount == true) {
					countResult = getResultCount(results,qryName,qryString,dirPath);
				}
				qexec.close();
	        } finally {
	            spatialDataset.end();
	        }	        
    	
	        return(new Pair(elapsedTime,countResult));		
     }
    private static long getResultCount(ResultSet QueryResult,String qryName,String qryString,String dirPath)
	{
		long cnt =0;	
		try { 
			BufferedWriter out = new BufferedWriter(new FileWriter(dirPath + "/JenaResults.txt", true)); 
            out.write("\n ++ Query (" + qryName + ") :: \n"); 
				while (QueryResult.hasNext()) {
					cnt++;
					QuerySolution soln = QueryResult.nextSolution() ;
				    RDFNode node = soln.get("result") ;       // Get a result variable by name.
				    String result = "Uknown Type";
				    if(node.isResource()) {
				    	result =  node.asResource().toString() ;  // Get a result variable - must be a node
				    }
				    else if(node.isLiteral()){
				    	result =  node.asLiteral().getLexicalForm() ;  // Get a result variable - must be a literal
				    }
				    out.write(result); 
					out.write("\n"); 
					System.out.println(qryName + "  " + cnt);
				}
			out.close(); 
		} 
        catch (IOException e) { 
            System.out.println("exception occoured" + e); 
        } 		
		return(cnt);				
	}
    public static void destroy(Dataset spatialDataset){

        SpatialIndex index = (SpatialIndex)spatialDataset.getContext().get(SpatialQuery.spatialIndex);
        if (index instanceof SpatialIndexLucene){
            deleteOldFiles(JenaUtils.indexDir);
            deleteOldFiles(JenaUtils.tdbDir);
        }
                
    }
    
    private static void deleteOldFiles(File indexDir) {
    	if (indexDir.exists())
            emptyAndDeleteDirectory(indexDir);
    }
    
    private static void emptyAndDeleteDirectory(File dir) {
        File[] contents = dir.listFiles() ;
        if (contents != null) {
            for (File content : contents) {
                if (content.isDirectory()) {
                    emptyAndDeleteDirectory(content) ;
                } else {
                    content.delete() ;
                }
            }
        }
        dir.delete() ;
    }

}
