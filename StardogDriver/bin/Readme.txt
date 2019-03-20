1 - Usage

java -cp Stardogtest.jar Run data=<path to RDF data file> repeat=<Query Iterations>

2 - Example

java -cp Stardogtest.jar Run data=/home/RDFData/CPMeta.rdf repeat=25

3 - Java version tested is Oracle JDK 1.8.0_201

4 - Output

	a. Database created in folder StardogTemp

        b. Performance produced in StardogTemp/StardogPerfs.txt

	c. Query results produced in StardogTemp/StardogResults.txt