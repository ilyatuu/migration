package iact.dev;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;


public class DbConnect {
	private static Properties props;
	// TODO Auto-generated method stub
	public Connection getConn() throws Exception{
		try{
			
			synchronized(DbConnect.class){
				if(props == null){
					InputStream in = this.getClass().getClassLoader().getResourceAsStream("db.properties");
					System.out.println("reading db.properties");
					props = new Properties();
					props.load(in);
					in.close();
				}
			}
			String driver = props.getProperty("db.driver"); 
			if (driver != null) {
				try{
					Class.forName("org.postgresql.Driver");
				}catch(ClassNotFoundException e){
					System.out.println("Missing JDBC Driver. Please include it in your library path");
					throw e;
				}
			}
			String db = props.getProperty("db.name");
            String url = props.getProperty("db.url");  
            String user = props.getProperty("db.user");  
            String pass = props.getProperty("db.pass");
    	        
            return DriverManager.getConnection(url+db, user, pass); 
    	        
		}catch(Exception e){
				throw e;
		}
	}

}
