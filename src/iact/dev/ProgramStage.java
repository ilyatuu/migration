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
 * Servlet implementation class ProgramStage
 */
@WebServlet("/ProgramStage")
public class ProgramStage extends HttpServlet {
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
    public ProgramStage() {
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
			case "ps":
				jreturn = checkProgramStage(cnn,request.getParameter("ps_uid").toString());
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
	protected JSONObject checkProgramStage(Connection cnn, String ps_uid) throws Exception{
		System.out.println("Verifying program stage exist, ps uid = " + ps_uid);
		json1 = new JSONObject();
		try{
			query = "select programid from programstage where uid ilike ?;";
			pstm = cnn.prepareStatement(query);
			pstm.setString(1, ps_uid);
			ResultSet rs = pstm.executeQuery();
			if(rs.next()){
				json1.put("success", true);
				json1.put("progid", rs.getInt("programid"));
			}
			else{
				json1.put("success", false);
				json1.put("message", "Program stage from the data file does not exist");
			}
			return json1;
		}catch(Exception e){
			throw e;
		}
	}

}
