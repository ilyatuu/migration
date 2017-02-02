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
	
	String query = "";
	String duser = "";
	String tablename = "";
	
	Date dateParsed;
	java.sql.Date dateSql;
	String dateString = "";
	
	int programid;
	JSONObject jReturn;
	JSONArray columns;
	JSONArray jdata;
	PrintWriter pw;
	SimpleDateFormat formatter = new SimpleDateFormat("mm/dd/yy",Locale.US);
       
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
		pw = response.getWriter();
		response.setContentType("application/json");
		
		
		tablename = request.getParameter("tablename");
		columns   = new JSONArray(request.getParameter("columns"));
		jReturn = new JSONObject();
		try{
			//1.1 Create temporary sequence for tracked instance id
			db = new DbConnect();
			cnn = db.getConn();
			//cnn.setAutoCommit(false);
			CreateTable(cnn);
			
			jReturn.put("success", true);
			jReturn.put("message", "Analytics table created!");
			pw.print(jReturn);
		}catch(SQLException e){
			//System.out.println( e.getNextException().toString() );
			pw.print(e.getLocalizedMessage());
			System.out.println( e.getLocalizedMessage() );
			e.printStackTrace();
		}catch(Exception e){
			pw.print(e.getLocalizedMessage());
			System.out.println( e.getLocalizedMessage() );
			e.printStackTrace();
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

	protected void CreateTable(Connection cnn) throws Exception{
		query = "DROP TABLE IF EXISTS "+tablename;
		pstm = cnn.prepareStatement(query);
		pstm.executeUpdate();
		
		query = "CREATE TABLE IF NOT EXISTS " + tablename + "( \n";
		try{
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
			
			//Create analytics table
			pstm = cnn.prepareStatement(query);
			pstm.executeUpdate();
			//cnn.commit();
			System.out.println("Analytics table structure created");
			
		}catch(Exception e){
			//cnn.rollback();
			throw e;
		}
	}
}
