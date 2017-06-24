package iact.dev;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Servlet implementation class InsertToAnalyticsTable
 */
@WebServlet(description = "Insert records into analytics table after creating it", urlPatterns = { "/InsertToAnalyticsTable" })
public class InsertToAnalyticsTable extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private DbConnect db;
	private Connection cnn = null;
	private PreparedStatement pstm = null;
	
	String query = "";
	String duser = "";
	String tablename = "";
	
	Date dateParsed;
	java.sql.Date dateSql;
	String dateString = "";
	
	int programid;
	JSONObject json;
	JSONObject jReturn;
	JSONArray columns;
	JSONArray jdata;
	PrintWriter pw;
	SimpleDateFormat formatter = new SimpleDateFormat("mm/dd/yy",Locale.US);
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public InsertToAnalyticsTable() {
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
		pw = response.getWriter();
		//response.setContentType("application/json");
		
		System.out.println( request.getParameterMap().isEmpty() );
		
		duser = request.getParameter("duser");
		tablename = request.getParameter("tablename");
		
		//System.out.println( request.getParameterMap().toString() );
		//JSONObject json = new JSONObject( request.getParameter("object") );
		//System.out.println( "table is "+json.getString("tablename") );
		//System.out.println( "program is "+ json.getString("program") );
		
		programid = Integer.parseInt( request.getParameter("program") );
		jdata	= new JSONArray(request.getParameter("rows"));
		columns	= new JSONArray(request.getParameter("columns"));
		
		
		jReturn = new JSONObject();
		try{
			db = new DbConnect();
			cnn = db.getConn();
			cnn.setAutoCommit(false);
			InsertIntoAnalyticsTable(cnn);
			pw.print(jReturn);
		}catch(SQLException e){
			pw.print(e.getLocalizedMessage());
			System.out.println( e.getNextException().toString() );
			System.out.println( e.getLocalizedMessage() );
			//e.printStackTrace();
		}catch(Exception e){
			pw.print(e.getLocalizedMessage());
			System.out.println( e.getLocalizedMessage() );
			//e.printStackTrace();
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
	protected void InsertIntoAnalyticsTable(Connection cnn) throws Exception {
		try{
			if (cnn==null){
				db= new DbConnect();
				cnn = db.getConn();	
			}
			//Reset query
			query = "INSERT INTO "+tablename+" VALUES (";
			for (int i=0; i<columns.length();i++) { 	
				query +="?,";							
			}
			
			query = query.substring(0,query.length() - 1) + ");"; //remove the last comma
			pstm = cnn.prepareStatement(query);
			
			final int batchSize = 1000;
			int count=0;
			for ( int i=0; i<jdata.length();i++){
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
						case "cYxGMLtawEq": //date_first_attendance
							dateString = jdata.getJSONArray(i).getString(j);
							dateString = dateString.replace(" 0:00","");
							dateParsed = formatter.parse(dateString);
							dateSql = new java.sql.Date(dateParsed.getTime());
							pstm.setDate(j+1, dateSql);
							break;
						case "XgYnDsaCEk4": //birth date
							dateString = jdata.getJSONArray(i).getString(j);
							dateString = dateString.replace(" 0:00","");
							dateParsed = formatter.parse(dateString);
							dateSql = new java.sql.Date(dateParsed.getTime());
							pstm.setDate(j+1, dateSql);
							break;
						case "enrollmentdate":
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
				pstm.addBatch();
				if(++count % batchSize == 0) {
					pstm.executeBatch();
				}
			}
			pstm.executeBatch();
			cnn.commit();
			//Print output
			jReturn.put("success", true);
			jReturn.put("message", Integer.toString(jdata.length()) + " records temporary stored");
			System.out.println("Insert into analytics table completed");
			//pw.print(jReturn);
		}catch(Exception e){
			//cnn.rollback();
			throw e;
		}
	}

}
