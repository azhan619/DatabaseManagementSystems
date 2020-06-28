import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
      try {
    
      // Load JDBC driver
      Class.forName("org.postgresql.Driver");
 
    } catch (ClassNotFoundException e) {
 
     
      e.printStackTrace();
      
 
    }


  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
     boolean result=false;
        

    try {
      
      //Make the connection to the database, ****** but replace <dbname>, <username>, <password> with your credentials ******
     
      connection = DriverManager.getConnection(URL,username,password);
 
    } catch (SQLException e) {
 
     
      e.printStackTrace();
     
 
    }
    

if (connection != null){

  result= true;
  


}else {
   result=false;

}
return result;
      
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      

       boolean result = false;
       try{

        connection.close();
       if (connection.isClosed()){

  result= true;
       }
     }

      catch (SQLException e) {
 
     
      e.printStackTrace();
     
 
    }

      

      return result;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
        boolean result =false;
       
        

        String sqlT = "SELECT * FROM country WHERE cid =  " + cid+";";
       
       String sqlText;
       
      
        sqlText = "INSERT INTO country(cid,cname,height,population) VALUES (" + cid + ",'" + name + "'," + height + "," + population + 
        ")";
       
        try {

             sql = connection.createStatement(); 
            
             rs= sql.executeQuery(sqlT);

             if(!rs.next()){
              sql = connection.createStatement(); 
              sql.executeUpdate(sqlText);
              

               if (sql.getUpdateCount() > 0) {

                result=true;
               };
             }
             else {

              result=false;
             }


            rs.close();             
            

            } 
        catch (SQLException e) {
            e.printStackTrace();        
            

            }


return result;

  
  }
  
  public int getCountriesNextToOceanCount(int oid) {

 int result=0;
 String sqlText;
    
    try { 
        
        sql = connection.createStatement();

    sqlText= "SELECT COUNT(cid) AS count FROM OceanAccess WHERE oid=" + oid + ";"; 
    
    rs = sql.executeQuery(sqlText);

    if(rs != null){

            while(rs.next()){

             result = rs.getInt(1) ;
            

    }
  }
    rs.close();
    }
    

catch(SQLException e){


    result=-1;
}

    
return result; 
  }
   
  public String getOceanInfo(int oid){
   
   String result = "";
    String sqlText;
    try {
            sql= connection.createStatement();

            sqlText = "SELECT * FROM ocean WHERE oid= " + oid;

            rs = sql.executeQuery(sqlText);
            

           if (rs != null){

              while(rs.next()){


                 result = rs.getInt(1)+":"+rs.getString(2)+":"+rs.getInt(3);
                

            }
        }
          rs.close();
    }

        catch(SQLException e){


            e.printStackTrace();

        }
        



 return result;





    }
    


  
  
  

  public boolean chgHDI(int cid, int year, float newHDI){
    boolean result=false;
    String sqlText;
    sqlText= "UPDATE hdi SET hdi_score=" + newHDI + " WHERE cid=" + cid +" AND year=" + year;

    try {
        sql = connection.createStatement();   
         sql.executeUpdate(sqlText);

         if ( sql.getUpdateCount() > 0){

                 result=true;
         }

             }

             catch (SQLException e) {

                e.printStackTrace();
          
              }
    
          return result;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
     boolean result = false;
   String sqlText;// country:number, neighbor:number, length:number
    sqlText = "DELETE FROM neighbour WHERE "+"("+ "country = "+c1id+ " AND "+" neighbor = "+c2id+")"+" OR "+"("+ "country = "+c2id+ " AND "+"neighbor = "+c1id+")";

    try {
      sql = connection.createStatement();
      sql.executeUpdate(sqlText);

      if (sql.getUpdateCount() > 0) {

        result = true;
      }

    }

    catch (SQLException e) {

      e.printStackTrace();

    }

    return result; 
  }
  
  public String listCountryLanguages(int cid){
	

  String result="";
    String sqlText;
    sqlText= "SELECT lid,lname,(population * (lpercentage/100)) AS population" 
            + " FROM country,language"
            +" WHERE country.cid=" + cid + " AND " + "language.cid=" + cid + " ORDER BY population DESC";
            try {
              sql = connection.createStatement();
              rs = sql.executeQuery(sqlText);
        int i = 1;
        if(rs != null){

             while(rs.next())
             {
               String m = "\n";
               if (i != 1){
               result+=m+rs.getInt(1)+":"+rs.getString(2)+":"+rs.getFloat(3);
               }
              else 
              {
              result+= rs.getInt(1)+":"+rs.getString(2)+":"+rs.getFloat(3);
              }
              i++;
             }
           }
           
                 rs.close();
           }

         
        
            
        
            catch (SQLException e) {
        
              e.printStackTrace();
        
            }
        
            return result;



}
  
  public boolean updateHeight(int cid, int decrH){
     boolean result=false;
    String sqlText;
    sqlText = "UPDATE country SET height= height - " + decrH + " WHERE cid=" + cid;

    try {
      sql = connection.createStatement();
      sql.executeUpdate(sqlText);

      if (sql.getUpdateCount() > 0) {

        result = true;
      }

    }

    catch (SQLException e) {

      result=false;

    }


    return result;
  }
    
  public boolean updateDB(){


boolean result=false;
   String sqlText;
   String sqlText1;

   sqlText = "CREATE TABLE mostPopulousCountries(" +

                "                               cid  INTEGER , cname VARCHAR(20) );        " ;

         sqlText1 =  " INSERT INTO mostPopulousCountries( SELECT cid,cname FROM country WHERE population>100000000 ORDER BY cid ASC)      ";

              
                try {
                  sql = connection.createStatement();
                  sql.executeUpdate(sqlText);
                  sql.executeUpdate(sqlText1);


                  if (sql.getUpdateCount() > 0) {

                    result = true;
                  }

                  
                 /* if (sql.getUpdateCount() > 0) {

                    result = true;
                  }*/
            
                }
            
                catch (SQLException e) {
            
                 
                  result= false;
            
                }             

return result;

                 }


 /*public static void main(String[] args){

Assignment2 test = new Assignment2();
boolean t = test.connectDB("jdbc:postgresql://db:5432/azhan619?currentSchema=a2","azhan619","215774466");

if(t){

  System.out.println("Works.");
}



  boolean a = test.insertCountry(25,"Morocco",8000,2000);

  if(a){

  System.out.println("2nd Works.");
}else {

 System.out.println("Alreaady exists");

}



int c = test.getCountriesNextToOceanCount(1);
System.out.println(c);






String new1 = test.getOceanInfo(2);
System.out.println(new1);

double ab=0.14;
boolean v = test.chgHDI(5,2011,(float) 0.14);

if(v){

  System.out.println("HDI Updated!");
}


boolean nei = test.deleteNeighbour(10,8);

if(nei){
System.out.println("Deleted!");


}
String d = test.listCountryLanguages(8);

   System.out.println(d);


boolean th = test.updateHeight(5,40);

if(th){
System.out.println("Height Updated!");

}


boolean y= test.updateDB();
if(y){

System.out.println("Table added");

}


boolean close= test.disconnectDB();

if(close){

System.out.println("Closed");

}


  }*/
  
}
