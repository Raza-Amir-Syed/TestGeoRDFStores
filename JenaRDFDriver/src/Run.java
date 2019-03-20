import java.io.BufferedWriter;
import java.io.File;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.jena.query.spatial.EntityDefinition;
import org.apache.jena.query.spatial.SpatialDatasetFactory;

import com.github.jsonldjava.core.RDFDataset.Literal;
//import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;

import org.apache.jena.query.* ;

public class Run {

	public static void main(String[] args) {
		
		DATASET_TYPES setType = DATASET_TYPES.TDB;
		//DATASET_TYPES setType = DATASET_TYPES.MEMORY
		
		String dirPath = System.getProperty("user.dir") + "/JenaTemp"; //"/home/amir/RDFTemps/JenaTemp";
		String RDFFile = "" ; //  "/home/amir/CPMeta.RDF";
		int queryIterations = 1;
		ArrayList<String> resultSet  = null;
		
		for(String cmdParam: args) {
			
			String keyValue[] = cmdParam.split("=");
			if(keyValue[0].toLowerCase().equals("data")) {
				RDFFile = keyValue[1];
			}
			else if (keyValue[0].toLowerCase().equals("repeat")) {
				queryIterations = Integer.parseInt(keyValue[1]) ;
			}
		}
		
		if(RDFFile=="") {
			System.out.println("\n Data File not provided. Usage is given below");
			System.out.println("\n java -cp Jenatest.jar Run data=<data_file> repeat=<test_iterations> ");
			System.out.println("\n java -cp JenaJtest.jar Run data=/home/CPMeta.rdf repeat=20 ");
			return;
		}
		
		
		try {
			resultSet = JenaTestDriver.runTest(setType, RDFFile, dirPath, queryIterations);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//write the result on console. Change here to write in a file or in a file
		/*for (String res:resultSet) {
			System.out.println(res);			
		}*/
		System.out.println("*********************  Test Peformance *********************");		
		try { 
			BufferedWriter out = new BufferedWriter(new FileWriter(dirPath + "/JenaPerfs.txt",false)); 			
			//write the result on console and file. Change here to write in a file or in a file
			for (String res:resultSet) {
				System.out.println(res);	
				out.write(res); 
				out.write("\n"); 
			}
			out.close();	
		} 
		
        catch (IOException e) { 
            System.out.println("exception occoured" + e); 
        } 
		

	}
	
	
	
	
/*	public static void spatialQuery() {
		
		EntityDefinition entDef = new EntityDefinition("entityField", "asWKT");
		entDef.setSpatialContextFactory("org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory");
		Path path = Paths.get("r:\\index\\jena.idx");
		Dataset spatialDataset=null;
		try {
			Directory dir = FSDirectory.open(path);
			
			//Model m = ModelFactory.createDefaultModel();
			//RDFDataMgr.read(m, "R:\\socat.rdf");
			
			spatialDataset = SpatialDatasetFactory.createLucene(DatasetFactory.create(), dir,entDef);			
			spatialDataset.begin(ReadWrite.WRITE);
			Model m = spatialDataset.getDefaultModel();			
			m.read(new FileInputStream("R:\\socat.rdf"),null);
			spatialDataset.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			spatialDataset.end();
		}
	}
*/
	/*public static void simpleQuery() {
		try {
			FileInputStream fi = new FileInputStream("R:\\socat.rdf");
			Model model = ModelFactory.createDefaultModel();
			System.out.println("Size = " + model.size());
			model.read(fi,null) ;
			System.out.println("Size = " + model.size());
			String queryString = "PREFIX cpmeta1: <http://meta.icos-cp.eu/ontologies/cpmeta/>\r\n" + 
					"PREFIX geo: <http://www.opengis.net/ont/geosparql#>\r\n" + 
					"PREFIX sf: <http://www.opengis.net/ont/sf#> \r\n" + 
					"PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/>\r\n" + 
					"PREFIX spatial: <http://jena.apache.org/spatial#>\r\n" + 
					"\r\n" + 
					"SELECT (count(?obj1) as ?C) \r\n" + 
					"WHERE {\r\n" + 
					"  ?obj1 geo:asWKT ?coord1.\r\n" + 
					"  ?coord1 spatial:nearby (18.5 54.9 100 'km')." +
					"}\r\n" ;
			Query query = QueryFactory.create(queryString) ;
			QueryExecution qexec = QueryExecutionFactory.create(query, model);
			ResultSet results = qexec.execSelect() ;
			for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode node = soln.get("C") ;       // Get a result variable by name.
		      String result =  node.asLiteral().getLexicalForm() ;   // Get a result variable - must be a literal
		      System.out.println(" Query count = " + result);
		    }
			qexec.close();
			model.close(); 
			fi.close();
	}
	catch(FileNotFoundException ex) {
		System.out.println(ex.getMessage());
	}
	catch(IOException ex) {
		System.out.println(ex.getMessage());
	}
	
	
	
	Model model = ModelFactory.createDefaultModel();
	model.read("socat.RDF") ;		
	model.close();
	}*/
}
