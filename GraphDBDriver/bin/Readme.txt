1 - Usage

java -cp GraphDBtest.jar Run data=<path to RDF data file> repeat=<Query Iterations> config=<DB config file>

2 - Example

java -cp GraphDBtest.jar Run data=/home/RDFData/CPMeta.rdf repeat=25 config=config.ttl

3 - Java version tested is Oracle JDK 1.8.0_201

4 - Output

	a. Database created in folder GraphDBTemp

        b. Performance produced in GraphDBTemp/GraphDBPerfs.txt

	c. Query results produced in GraphDBTemp/GraphDBResults.txt