package iact.dev;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Servlet implementation class ProcessNewRec
 */
@WebServlet("/ProcessNewRec")
public class ProcessNewRec extends HttpServlet {
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
    public ProcessNewRec() {
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
		
		
		duser = request.getParameter("duser");
		tablename = request.getParameter("tablename");
		programid = Integer.parseInt( request.getParameter("program") );
		jdata	= new JSONArray(request.getParameter("rows"));
		columns = jdata.getJSONArray(0);
		
		jReturn = new JSONObject();
		try{
			db = new DbConnect();
			cnn = db.getConn();
			cnn.setAutoCommit(false);
			InsertIntoAnalyticsTable(cnn);
			InsertNewTrackedEntityInstance(cnn);
			InserttrackedEntityAttributes(cnn);
			
			jReturn.put("success", true);
			jReturn.put("message", "Data migration completed");
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
			pstm.executeBatch();
			cnn.commit();
			//Print output
			jReturn.put("success", true);
			jReturn.put("message", Integer.toString(jdata.length()) + " records temporary stored");
			//pw.print(jReturn);
		}catch(Exception e){
			//cnn.rollback();
			throw e;
		}
	}
	protected void InsertNewTrackedEntityInstance(Connection cnn) throws Exception{
		try{
			query = "CREATE TEMP SEQUENCE IF NOT EXISTS temp_trackedinstanceid_seq INCREMENT BY 1START 1;";
			pstm = cnn.prepareStatement(query);
			pstm.execute();
			
			//1.2. Create temporary table for programinstanceid
			query  = "CREATE TEMP TABLE temp_trackedentityinstance(";
			query += "trackedentityinstanceid integer NOT NULL DEFAULT nextval('temp_trackedinstanceid_seq'),";
			query += "uid character varying(11),";
			query += "organisationunitid integer NOT NULL,";
			query += "trackedentityid integer,";
			query += "CONSTRAINT trackedentityinstance_pkey PRIMARY KEY (trackedentityinstanceid)";
			query += ");";
			pstm = cnn.prepareStatement(query);
			pstm.execute();
			
			//Set table sequency to last value of the table record in the original table
			query  = "SELECT setval('temp_trackedinstanceid_seq',";
			query += "COALESCE((SELECT MAX(trackedentityinstanceid)+1 FROM trackedentityinstance), 1), false);";
			pstm = cnn.prepareStatement(query);
			pstm.execute();
			
			
			//379594 is trackedentity of the type Person. This is under the table trackedentity
			query   = "INSERT INTO temp_trackedentityinstance";
			query  += "(uid,organisationunitid,trackedentityid)";
			query  += "SELECT ";
			query  += "an.tei as uid,";
			query  += "ou.organisationunitid,";
			query  += "'379594' as trackedidentityid ";
			query  += "from "+tablename+" an ";
			query  += "left join organisationunit ou on an.ou = ou.uid;";
			pstm = cnn.prepareStatement(query);
			pstm.execute();
			
			pstm = cnn.prepareStatement(query);
			pstm.execute();
			
			
			query  = "INSERT INTO trackedentityinstance(";
			query += "trackedentityinstanceid, uid, code, created, lastupdated,";
			query += "representativeid,organisationunitid, trackedentityid) ";
			query += "SELECT trackedentityinstanceid, uid,NULL,now()::timestamp(3) as created,now()::timestamp(3) as lastupdated,";
			query += "NULL,";
			query += "organisationunitid,trackedentityid ";
			query += "FROM temp_trackedentityinstance;";
			
			pstm  = cnn.prepareStatement(query);
			pstm.execute();
			cnn.commit();
			System.out.println("trackedentityinstance insert completed");
		}catch(Exception e){
			//cnn.rollback();
			throw e;
		}
	}
	protected void InserttrackedEntityAttributes(Connection cnn) throws Exception{
		try{
			query  = "insert into trackedentityattributevalue ";
			query += "select distinct on (t3.trackedentityinstanceid, t4.trackedentityattributeid) ";
			query += "t3.trackedentityinstanceid, t4.trackedentityattributeid, t3.colvalu as value ";
			query += "from (";
			query += "select ";
			query += "tei,";
			query += "trackedentityinstanceid,";
			query += "unnest(array"+SpliceJSONArray(columns,"psi").toString().replace("\"", "'")+") as colname,";
			query += "unnest(array"+SpliceJSONArray(columns,"psi").toString().replace("\"", "")+") as colvalu ";
			query += "from "+tablename+"  t1 ";
			query += "left join trackedentityinstance  t2 on t1.tei ilike t2.uid";
			query += ") t3 left join trackedentityattribute t4 on t3.colname ilike t4.uid ";
			query += "WHERE t3.colvalu is not null and t4.trackedentityattributeid is not null;";
			
			pstm  = cnn.prepareStatement(query);
			pstm.execute();
			cnn.commit();
			System.out.println("trackedEntityattributes insert completed");
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected void InsertProgramInstance(Connection cnn) throws Exception{
		try{
			query = "CREATE TEMP SEQUENCE IF NOT EXISTS temp_programinstance_seq INCREMENT BY 1 START 1;";
			pstm  = cnn.prepareStatement(query);
			pstm.execute();
			
			query  = "CREATE TEMP TABLE temp_programinstance";
			query += "(";
			query += "programinstanceid integer NOT NULL DEFAULT nextval('temp_programinstance_seq'),";
			query += "programid integer NOT NULL,";
			query += "uid character varying(11),";
			query += "trackedentityinstanceid integer,";
			query += "organisationunitid integer,";
			query += "CONSTRAINT programinstance_pkey PRIMARY KEY (programinstanceid)";
			query += ");";
			pstm  = cnn.prepareStatement(query);
			pstm.execute();
			
			query = "SELECT setval('temp_programinstance_seq', COALESCE((SELECT MAX(programinstanceid)+1 FROM programinstance), 1), false);";
			pstm  = cnn.prepareStatement(query);
			pstm.execute();
			
			query  = "INSERT INTO temp_programinstance(programid,uid,trackedentityinstanceid,organisationunitid)";
			query +="SELECT distinct on (pi) "+ programid + ",";
			query += "pi,t3.trackedentityinstanceid,t2.organisationunitid ";
			query += "from "+tablename+" t1 ";
			query += "left join organisationunit t2 on t1.ou = t2.uid ";
			query += "left join trackedentityinstance t3 on t1.tei = t3.uid ";
			query += "group by tei,pi,trackedentityinstanceid,t2.organisationunitid;";
			
			pstm  = cnn.prepareStatement(query);
			pstm.execute();
			
			query = "insert into programinstance(";
			query += "programinstanceid, enrollmentdate, enddate, programid,";
			query += "status, followup, trackedentitycommentid, uid, created, lastupdated,";
			query += "trackedentityinstanceid, patientid, organisationunitid,incidentdate)";
			query += "select ";
			query += "programinstanceid,now()::timestamp(3),NULL,programid,";
			query += "0,'f',NULL,uid,now()::timestamp(3),now()::timestamp(3),trackedentityinstanceid,";
			query += "NULL,organisationunitid,now()::timestamp(3)";
			query += "from temp_programinstance";
			pstm  = cnn.prepareStatement(query);
			pstm.execute();
			cnn.commit();
			System.out.println("programinstance insert completed");
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected String getUiD(int iLength) {
        String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < iLength) {
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }
	protected JSONArray SpliceJSONArray(JSONArray jarr, String SpliceAt){
		JSONArray j = new JSONArray();
		for(int i=0;i<jarr.length();i++){
			if(jarr.getString(i).equalsIgnoreCase(SpliceAt))
				break;
			j.put(jarr.getString(i));
		}
		return j;
	}

}
