
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.GraphUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

public class Run {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		/*
		
		System.out.println("++++++++++++++++++++++++ Query");
		String SPARQLQuery = " PREFIX geo: <http://www.opengis.net/ont/geosparql#> " +
                " PREFIX sf: <http://www.opengis.net/ont/sf#> " +
                " PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " + 	
                " SELECT ?geom1 ?geom2			" + 
                " WHERE {										" + 
                "  ?geom1 a sf:Polygon;							" + 
                "         geo:asWKT ?result.					" + 
                "  ?geom2 a sf:Polygon;							" + 
                "         geo:asWKT ?coord.					    " + 
                "  FILTER(?geom1!=?geom2).						" +
                "  FILTER(geof:sfOverlaps(?result,?coord))		" +
                " }" ;
		Pair p1 = GraphDBUtils.runSPARQL(repo, "Q1", SPARQLQuery);
		
		System.out.println("++++++++++++++++++++++++ Re-Indexing");
		GraphDBUtils.reIndex(repo);
		
		System.out.println("++++++++++++++++++++++++ Query 2");
		SPARQLQuery = " PREFIX geo: <http://www.opengis.net/ont/geosparql#> " +
                " PREFIX sf: <http://www.opengis.net/ont/sf#> " +
                " PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " + 	
                " SELECT ?geom1 ?geom2			" + 
                " WHERE {										" + 
                "  ?geom1 a sf:Polygon;							" + 
                "         geo:asWKT ?result.					" + 
                "  ?geom2 a sf:Polygon;							" + 
                "         geo:asWKT ?coord.					    " + 
                "  ?geom1 geo:sfOverlaps ?geom2. " +
                "  FILTER(?geom1!=?geom2).						" +
                " }" ;
		Pair p2 =  GraphDBUtils.runSPARQL(repo, "Q2", SPARQLQuery);
							
		System.out.println("++++++++++++++++++++++++ Shutting Down Repo");
		repo.shutDown();
		System.out.println("++++++++++++++++++++++++ Shutting Down Manager");
		repoManager.shutDown();*/
		
		/*System.out.println(p1.getCount() + "," + p1.getTime());
		System.out.println(p2.getCount() + "," + p2.getTime());*/
		
		String configFile = System.getProperty("user.dir") + "/config.ttl";
		String dataFile   = "" ; //"E:\\gfdb\\CPMeta.RDF";
		String storageLocation = "GraphDBTemp" ; // "E:\\gfdb\\storage";
		int iterations = 1;
		
		for(String cmdParam: args) {
			
			String keyValue[] = cmdParam.split("=");
			if(keyValue[0].toLowerCase().equals("data")) {
				dataFile = keyValue[1];
			}
			else if (keyValue[0].toLowerCase().equals("repeat")) {
				iterations = Integer.parseInt(keyValue[1]) ;
			}
			else if (keyValue[0].toLowerCase().equals("config")) {
				configFile = keyValue[1];
			}
		}
		
		if(dataFile=="") {
			System.out.println("\n Data File not provided. Usage is given below");
			System.out.println("\n java -cp GraphDBtest.jar Run data=<data_file> repeat=<test_iterations> config=<config_file>");
			System.out.println("\n java -cp GraphDBtest.jar Run data=/home/CPMeta.rdf repeat=20 config=/home/config.ttl");
			return;
		}
		
		GraphDBTestDriver.initializeTestPlatform(configFile,dataFile,storageLocation);
		
		
		ArrayList<String> resultSet1 = GraphDBTestDriver.runTest(iterations,false,storageLocation);		
		ArrayList<String> resultSet2 = GraphDBTestDriver.runTest(iterations,true,storageLocation);		
		GraphDBTestDriver.shutDown();
		
		System.out.println("*********************  Test Peformance *********************");			
		try { 
			BufferedWriter out = new BufferedWriter(new FileWriter(storageLocation + "/GraphDBPerfs.txt",false)); 			
			//write the result on console and file. Change here to write in a file or in a file
			for (String res:resultSet1) {
				System.out.println(res);	
				out.write(res); 
				out.write("\n"); 
			}
			System.out.println();
			out.write("\n");
			for (String res:resultSet2) {
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

}
