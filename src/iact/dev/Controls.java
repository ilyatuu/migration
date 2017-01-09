package iact.dev;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Servlet implementation class Controls
 */
@WebServlet("/Controls")
public class Controls extends HttpServlet {
	private static final long serialVersionUID = 1L;
	JSONObject json1;
	JSONObject json2;
	JSONArray  jarr;
	JSONObject jreturn;
	String query = "";
	
	DbConnect db;
	PreparedStatement pstm = null;
	Connection cnn = null;
	ResultSet rs;
	PrintWriter pw;
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public Controls() {
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
		try{
			
			String rtype = request.getParameter("rtype");
			db = new DbConnect();
			cnn = db.getConn();
			switch(rtype){
			case "chkuser":
				jreturn = chkUser(cnn,request.getParameter("usr").toString());
				break;
			case "programs":
				jreturn = getPrograms(cnn);
				break;
			}
		//return
		response.setContentType("application/json");
		pw.print(jreturn);
		}catch(SQLException e){
			e.printStackTrace();
			jreturn.put("success", false);
			jreturn.put("message", e.getNextException().toString());
		}catch(Exception e){
			e.printStackTrace();
			jreturn.put("success", false);
			jreturn.put("message", e.toString());
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
	protected JSONObject getPrograms(Connection cnn) throws Exception{
		json1 = new JSONObject();
		jarr = new JSONArray();
		
		try{
			query = "select programid as id,name from program;";
			pstm = cnn.prepareStatement(query);
			rs = pstm.executeQuery();
			while(rs.next()){
				json2 = new JSONObject();
				json2.put("id", rs.getInt("id"));
				json2.put("name", rs.getString("name"));
				jarr.put(json2);
			}
			
			json1.put("success", true);
			json1.put("message", "Retrieved program id and name");
			json1.put("program", jarr);
			return json1;
		}catch(Exception e){
			throw e;
		}
	}
	protected JSONObject chkUser(Connection cnn, String uname) throws Exception{
		System.out.println("Verifying if user exist, username: "+uname);
		json1 = new JSONObject();
		try{
			query = "select * from users where username like ?;";
			pstm = cnn.prepareStatement(query);
			pstm.setString(1, uname);
			ResultSet rs = pstm.executeQuery();
			if(rs.next())
				json1.put("success", true);
			else
				json1.put("success", false);
			return json1;
		}catch(Exception e){
			throw e;
		}
	}

}
