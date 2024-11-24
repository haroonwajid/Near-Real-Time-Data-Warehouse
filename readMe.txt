Follow the steps below to run the whole project:

1. Open SQL Workbench
2. Open "Create-DW" file and run the script.
3. Open VS Code and open the project zip folder named "haroon_21i1763_project".
4. Open the src folder where file "MeshJoin.java" and the QueryExecuter.java file is located and run it by the commands:
	
	javac -cp ".:../lib/mysql-connector-j-9.1.0.jar" MeshJoin.java QueryExecutor.java

        java -cp ".:../lib/mysql-connector-j-9.1.0.jar" MeshJoin 

5. The output will ask for credentials, load data into warehouse aswell as query output. 
6. The execution will also show the SQL Queries.
7. Similarly execute the olap_queries.sql in workbench to view output.
