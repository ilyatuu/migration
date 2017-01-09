package iact.dev;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Date;
import java.util.Locale;
import java.text.SimpleDateFormat;

/**
 * Servlet implementation class CreateAnalyticsTable
 */
@WebServlet(description = "Creates analytics table", urlPatterns = { "/CreateAnalyticsTable" })
public class CreateAnalyticsTable extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	private DbConnect db;
	private Connection cnn = null;
	private PreparedStatement pstm = null;
	
	JSONObject jReturn;
	PrintWriter pw;
	
	SimpleDateFormat formatter = new SimpleDateFormat("mm/dd/yy",Locale.US);
	
	Date dateParsed;
	java.sql.Date dateSql;
	String dateString = "";
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public CreateAnalyticsTable() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		
		response.setContentType("application/json");
		try{
			String tablename = request.getParameter("tablename");
			JSONArray columns = new JSONArray(request.getParameter("columns"));
			JSONArray jdata	= new JSONArray(request.getParameter("data"));
			
			
			jReturn = new JSONObject();
			pw = response.getWriter();
			
			
			//System.out.println(columns);
			//System.out.println(jdata);
			
			String query = "DROP TABLE IF EXISTS "+tablename;
			db = new DbConnect();
			cnn = db.getConn();
			cnn.setAutoCommit(false);
			pstm = cnn.prepareStatement(query);
			pstm.executeUpdate();
			
			query = "CREATE TABLE IF NOT EXISTS " + tablename + "( \n";
			
			for (int i=0; i<columns.length();i++) {
				
				switch(columns.getString(i)){
				case "psi":
					query += columns.getString(i) + " character(11) NOT NULL,\n";
					break;
				case "pi":
					query += columns.getString(i) + " character(11) NOT NULL,\n";
					break;
				case "ps":
					query += columns.getString(i) + " character(11) NOT NULL,\n";
					break;
				case "executiondate":
					query += columns.getString(i) + " timestamp without time zone,\n";
					break;
				case "longitude":
					query += columns.getString(i) + " double precision,\n";
					break;
				case "latitude":
					query += columns.getString(i) + " double precision,\n";
					break;
				case "ou":
					query += columns.getString(i) + " character(11) NOT NULL,\n";
					break;
				case "tei":
					query += columns.getString(i) + " character(11),\n";
					break;
				default:
					query += columns.getString(i) + " character varying(100),\n";
					break;
				}
				
			}
			query = query.substring(0,query.length() - 2); //remove the last comma
			query+= ");";
			//System.out.println(query);
			
			pstm = cnn.prepareStatement(query);
			pstm.executeUpdate();
			
			System.out.println("Analytics table structure created");
			
			//Reset query
			query = "INSERT INTO "+tablename+" VALUES (";
			for (int i=0; i<columns.length();i++) { 	
				query +="?,";							
			}
			query = query.substring(0,query.length() - 1) + ");"; //remove the last comma
			pstm = cnn.prepareStatement(query);
			final int batchSize = 1000;
			int count=0;
			
			for ( int i=1; i<jdata.length();i++){
				for(int j=0;j<jdata.getJSONArray(i).length();j++){
					if(jdata.getJSONArray(i).getString(j).isEmpty()){
						switch(columns.getString(j)){
						case "executiondate":
							pstm.setDate(j+1, null);
							break;
						case "longitude":
							pstm.setNull(j+1, java.sql.Types.DOUBLE);
							break;
						case "latitude":
							pstm.setNull(j+1, java.sql.Types.DOUBLE);
							break;
						default:
							pstm.setString(j+1, null);
							break;
						}
					}else{
						switch(columns.getString(j)){
						case "executiondate":
							dateString = jdata.getJSONArray(i).getString(j);
							dateString = dateString.replace(" 0:00","");
							dateParsed = formatter.parse(dateString);
							dateSql = new java.sql.Date(dateParsed.getTime());
							pstm.setDate(j+1, dateSql);
							break;
						case "longitude":
							pstm.setDouble(j+1, Double.parseDouble(jdata.getJSONArray(i).getString(j)));
							break;
						case "latitude":
							pstm.setDouble(j+1, Double.parseDouble(jdata.getJSONArray(i).getString(j)));
							break;
						default:
							pstm.setString(j+1, jdata.getJSONArray(i).getString(j));
							break;
						}
					}
					
				}
				//System.out.println(pstm.toString());
				pstm.addBatch();
				if(++count % batchSize == 0) {
					pstm.executeBatch();
				}
			}
			//System.out.println(pstm.toString());
			pstm.executeBatch();
			cnn.commit();
			//cnn.commit(); //Cannot commit when autoCommit is enabled
			//Print output
			jReturn.put("success", true);
			jReturn.put("message", Integer.toString(jdata.length()) + "Records processed");
			pw.print(jReturn);
			
		}catch(SQLException e){
			e.printStackTrace();
			jReturn.put("success", false);
			jReturn.put("message", e.getNextException().toString());
			pw.print(jReturn);
		}catch(Exception e){
			e.printStackTrace();
			jReturn.put("success", false);
			jReturn.put("message", e.toString());
			pw.print(jReturn);
		}finally{
			if (pstm != null) {
		        try {
		        	pstm.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		        }
		    }
		    if (cnn != null) {
		        try {
		            cnn.close();
		        } catch (SQLException e) {
		            e.printStackTrace();
		        }
		    }
		}
		
	}

}
