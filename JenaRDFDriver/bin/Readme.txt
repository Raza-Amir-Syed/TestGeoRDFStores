1 - Usage

java -cp Jenatest.jar Run data=<path to RDF data file> repeat=<Query Iterations>

2 - Example

java -cp Jenatest.jar Run data=/home/RDFData/CPMeta.rdf repeat=25

3 - Java version tested is Oracle JDK 1.8.0_201

4 - Output

	a. RDF4J repository created in folder JenaTemp

        b. Performance produced in JenaTemp/JenaPerfs.txt

	c. Query results produced in JenaTemp/JenaResults.txt