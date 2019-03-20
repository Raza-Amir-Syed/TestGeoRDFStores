

import java.util.ArrayList;

public class SPARQLQuerySet {
	
	private static String queryPrefix;
	static {
		StringBuilder strPrefix = new StringBuilder();
		strPrefix.append("PREFIX : <http://example/>");
		strPrefix.append("PREFIX cp: <http://meta.icos-cp.eu/ontologies/cpmeta/>");
		strPrefix.append("PREFIX geo: <http://www.opengis.net/ont/geosparql#>");
		strPrefix.append("PREFIX spatial: <http://jena.apache.org/spatial#>");
		strPrefix.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>");
		strPrefix.append("PREFIX sf: <http://www.opengis.net/ont/sf#>");
		queryPrefix = strPrefix.toString();
	}
		
	private ArrayList<SPARQLQuerySetMember> querySet = new ArrayList<SPARQLQuerySetMember>();
	
	public int getQueryCount() {
		return(querySet.size());
	}
	
	public SPARQLQuerySetMember getItem(int index) {
		
		return(querySet.get(index));
	}
	public SPARQLQuerySet() {
		
		SPARQLQuerySetMember Q1 = new SPARQLQuerySetMember("Q9:Intersect=>Lines-aBox",queryPrefix,			
				"SELECT ?result                                                   " + 
				"WHERE {                                                          " +				
				"    ?result ?b ?c          " + 				
				"}" );	
		SPARQLQuerySetMember Q2 = new SPARQLQuerySetMember("Q9:Intersect=>Lines-aBox",queryPrefix,			
				"SELECT ?result                                                " + 
				"WHERE {                                                          " +				
				"      ?a    geo:asWKT ?result .    " +
				"}" );		
		
		
		SPARQLQuerySetMember Q9 = new SPARQLQuerySetMember("Q9:Intersect=>Lines-aBox",queryPrefix,			
				"SELECT ?result                                                   " + 
				"WHERE {                                                          " +				
				"    ?geom spatial:intersectBox (54.0 14.0 59.0 23.0).          " + 
				"    ?geom a sf:LineString;									  " +
				"          geo:asWKT ?result.    " +
				"}" );		
		
		SPARQLQuerySetMember Q10 = new SPARQLQuerySetMember("Q10:Intersect=>Polygons-aBox",queryPrefix,			
				"SELECT ?result                                                   " + 
				"WHERE {                                                          " +				
				"    ?geom spatial:intersectBox (54.0 14.0 59.0 23.0).          " +
				"    ?geom a sf:Polygon;											  "	+
				"          geo:asWKT ?result.    " +
				"}" );
		
		SPARQLQuerySetMember Q13 = new SPARQLQuerySetMember("Q13:Within=>Points-aBox",queryPrefix,			
				"SELECT ?result                                                   " + 
				"WHERE {                                                          " +
				"    ?geom spatial:withinBox (54.0 14.0 59.0 23.0).             " + 
				"    ?geom a sf:Point;					                      " + 
				"          geo:asWKT ?result.    " +
				"}" );
		
		SPARQLQuerySetMember Q15 = new SPARQLQuerySetMember("Q15:Nearby=>Points-aPoint",queryPrefix,			
				"SELECT ?result                                                   " + 
				"WHERE {                                                          " + 
				"    ?geom spatial:nearby (57.430061 18.984339 500000.0 'metres').     " + 
				"    ?geom a sf:Point;										  " +
				"        geo:asWKT ?result.    " +
				"}" );
		querySet.add(Q1);
		querySet.add(Q2);
		querySet.add(Q9);
		querySet.add(Q10);
		querySet.add(Q13);
		querySet.add(Q15);
	
	}
}