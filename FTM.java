// Pablo Volpe | A11717425

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FTM_A11717425 
{
   public static void main (String[] args) throws ClassNotFoundException
   {

      // load the sqlite-JDBC driver using the current class loader
      Class.forName("org.sqlite.JDBC");

      Connection connection = null;
     
      try
      {
         // create a database connection
         // args[0] = database file
         connection = DriverManager.getConnection("jdbc:sqlite:" + args[0]);
         Statement statement = connection.createStatement();
         statement.setQueryTimeout(30); // set timeout to 30 sec


	 // create auxilary tables
         statement.executeUpdate("DROP TABLE IF EXISTS customerLookup");         
         statement.executeUpdate("CREATE TABLE customerLookup AS " + 
                                 "SELECT no, name " +
                                 "FROM account, customer, depositor " +
                                 "WHERE name = cname AND no = ano");
 
         statement.executeUpdate("DROP TABLE IF EXISTS funds");         
         statement.executeUpdate("CREATE TABLE funds AS " + 
                                 "SELECT cl1.name AS srcName, cl2.name AS tgtName " +
                                 "FROM customerLookup cl1, customerLookup cl2, transfer " +
                                 "WHERE transfer.src = cl1.no AND transfer.tgt = cl2.no");



         statement.executeUpdate("DROP TABLE IF EXISTS delta");
         statement.executeUpdate("CREATE TABLE delta ([from] VARCHAR(100)," + 
                                                      "[to] VARCHAR(100))");
	 
	 // create influence table + auxillary influence tables
         statement.executeUpdate("DROP TABLE IF EXISTS influence");
         statement.executeUpdate("CREATE TABLE influence ([from] VARCHAR(100)," + 
                                                          "[to] VARCHAR(100)," + 
                                                          "FOREIGN KEY ([from]) REFERENCES customer (name) ON DELETE CASCADE," +
                                                          "FOREIGN KEY ([to]) REFERENCES customer(name) ON DELETE CASCADE )");
 

         statement.executeUpdate("DROP TABLE IF EXISTS temp_influence");
         statement.executeUpdate("CREATE TABLE temp_influence ([from] VARCHAR(100)," + 
                                                               "[to] VARCHAR(100)," + 
                                                               "FOREIGN KEY ([from]) REFERENCES customer (name) ON DELETE CASCADE," +
                                                               "FOREIGN KEY ([to]) REFERENCES customer(name) ON DELETE CASCADE )");

         statement.executeUpdate("DROP TABLE IF EXISTS old_influence");
         statement.executeUpdate("CREATE TABLE old_influence ([from] VARCHAR(100)," + 
                                                              "[to] VARCHAR(100)," + 
                                                              "FOREIGN KEY ([from]) REFERENCES customer (name) ON DELETE CASCADE," +
                                                              "FOREIGN KEY ([to]) REFERENCES customer(name) ON DELETE CASCADE )");


         /* Begin semi-naive transitive closure algorithm*/
	 /* T = influence */
         /* G = funds */
         /* delta = change of influences */

         //instantiate influence table from funds
         // T = G
         statement.executeUpdate("INSERT INTO influence " +
                                 "SELECT * FROM funds");

         // delta = G
         statement.executeUpdate("INSERT INTO delta " +
                                 "SELECT * FROM funds");


         
         ResultSet countSet = statement.executeQuery("SELECT COUNT(*) AS count FROM delta");
	 int count = countSet.getInt("count");
	
         // while delta table has entries
	 while (count > 0) {
       
           
           // T_old = T 
           statement.executeUpdate("DELETE FROM old_influence");
	   statement.executeUpdate("INSERT INTO old_influence " +
                               "SELECT * FROM influence");

           
           
	   // T = T UNION (Connections from G and delta)
           statement.executeUpdate("DELETE FROM temp_influence");
	   statement.executeUpdate("INSERT INTO temp_influence " +
                                   "SELECT * FROM influence");

           statement.executeUpdate("DELETE FROM influence");
           statement.executeUpdate("INSERT INTO influence " +
                                   "SELECT * FROM temp_influence " + 
                                   "UNION "  +
                                   	"SELECT x.srcName, y.[to] " +
                                   		"FROM funds x, delta y " + 
                                   		"WHERE x.tgtName = y.[from]" );

           // delta = T - T_old
           statement.executeUpdate("DELETE FROM delta");
           statement.executeUpdate("INSERT INTO delta " +
                               "SELECT * FROM influence " +
                               "EXCEPT " +
                               "SELECT * FROM old_influence");



           // update count of delta
           countSet = statement.executeQuery("SELECT COUNT(*) AS count FROM delta");
           count = countSet.getInt("count");
	}

       /* 

       		Not Needed for final submission


      	 	// Output T
       		ResultSet rs = statement.executeQuery("SELECT * FROM influence WHERE [from] != [to]");

       
       		System.out.println("src\t\ttgt");
         	while (rs.next())
         	{
            
			// read the result set
            		System.out.println(rs.getString("from") + "\t\t" + rs.getString("to") );
         	}
        
       */


        
       // drop auxillary tables
       statement.executeUpdate("DROP TABLE IF EXISTS old_influence");
       statement.executeUpdate("DROP TABLE IF EXISTS temp_influence");
       statement.executeUpdate("DROP TABLE IF EXISTS funds");
       statement.executeUpdate("DROP TABLE IF EXISTS customerLookup");
       statement.executeUpdate("DROP TABLE IF EXISTS delta");
      }


      catch(SQLException e)
      {
         // if the error message is "out of memory"
         // it probably means no databasefile is found
         System.err.println(e.getMessage());
      }
      finally
      {
         try
         {
            if(connection != null)
               connection.close();
         }
         catch(SQLException e)
         {
            // connection close failed
            System.err.println(e);
         }
      }
   }
}
   
