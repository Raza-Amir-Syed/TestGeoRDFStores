1 - Usage

java -cp RDF4Jtest.jar Run data=<path to RDF data file> repeat=<Query Iterations>

2 - Example

java -cp RDF4Jtest.jar Run data=/home/RDFData/CPMeta.rdf repeat=25

3 - Java version tested is Oracle JDK 1.8.0_201

4 - Output

	a. RDF4J repository created in folder RDF4JTemp

        b. Performance produced in RDF4JTemp/RDF4JPerfs.txt

	c. Query results produced in RDF4JTemp/RDF4JResults.txt