1 - It requires a Virtuoso server running before executing this program

2 - Usage

java -cp Virtuosotest.jar Run data=<path to RDF data file> repeat=<Query Iterations> host=<hostname/IP> port=<Virtuoso server port> user=<virtuoso user> password=<user password> 

3 - Example

java -cp Virtuosotest.jar Run data=/home/CPMeta.RDF repeat=25 host=localhost port=1111 user=dba password=dba



4 - Java version tested is Oracle JDK 1.8.0_201

5 - Output

        a. Performance produced in VirtuosoPerfs.txt

	b. Query results produced in VirtuosoResults.txt

