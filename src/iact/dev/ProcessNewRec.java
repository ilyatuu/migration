package iact.dev;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
		columns	= new JSONArray(request.getParameter("columns"));
		
		jReturn = new JSONObject();
		try{
			db = new DbConnect();
			cnn = db.getConn();
			cnn.setAutoCommit(false);
			InsertTrackedEntityInstance(cnn);
			InserttrackedEntityAttributes(cnn);
			InsertProgramInstance(cnn);
			InsertProgramStageInstance(cnn);
			InsertTrackedEntityDataValue(cnn);
			DropAnalyticsTable(cnn);
			System.out.println("Migration process completed");
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
	
	
	protected void InsertTrackedEntityInstance(Connection cnn) throws Exception{
		try{
			query = "CREATE TEMP SEQUENCE IF NOT EXISTS temp_trackedentityinstanceid_seq INCREMENT BY 1 START 1;";
			pstm = cnn.prepareStatement(query);
			pstm.execute();
			
			//1.2. Create temporary table for programinstanceid
			query  = "CREATE TEMP TABLE temp_trackedentityinstance(";
			query += "trackedentityinstanceid integer NOT NULL DEFAULT nextval('temp_trackedentityinstanceid_seq'),";
			query += "uid character varying(11),";
			query += "organisationunitid integer NOT NULL,";
			query += "trackedentityid integer,";
			query += "CONSTRAINT trackedentityinstance_pkey PRIMARY KEY (trackedentityinstanceid)";
			query += ");";
			
			pstm = cnn.prepareStatement(query);
			pstm.execute();
			
			//Set table sequency to last value of the table record in the original table
			query  = "SELECT setval('temp_trackedentityinstanceid_seq',";
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
				
			//query  = "insert into trackedentityattributevalue ";
			query  = "select distinct on (t3.trackedentityinstanceid, t4.trackedentityattributeid) ";
			query += "t3.trackedentityinstanceid, t4.trackedentityattributeid, t3.colvalu as attributevalue ";
			query += "from (";
			query += "select ";
			query += "tei,";
			query += "trackedentityinstanceid,";
			query += "unnest(array"+RemoveUnwanted(columns).toString().replace("\"", "'")+") as colname,";
			query += "unnest(array"+RemoveUnwanted(columns).toString().replace("\"", "")+") as colvalu ";
			query += "from "+tablename+"  t1 ";
			query += "left join trackedentityinstance  t2 on t1.tei ilike t2.uid";
			query += ") t3 left join trackedentityattribute t4 on t3.colname ilike t4.uid ";
			query += "WHERE t3.colvalu is not null and t4.trackedentityattributeid is not null;";
			
			pstm  = cnn.prepareStatement(query);
			ResultSet rs = pstm.executeQuery();
			
			query  = "insert into trackedentityattributevalue values (?,?,?);";
			pstm = cnn.prepareStatement(query);
			final int batchSize = 1000;
			int count=0;
			
			while(rs.next()){
				
				pstm.setInt(1, rs.getInt("trackedentityinstanceid"));
				pstm.setInt(2, rs.getInt("trackedentityattributeid"));
				
				if(rs.getString("attributevalue").length() == 20 || rs.getString("attributevalue").contains(":")){
					pstm.setString(3, SQLDate(rs.getString("attributevalue")));
				}else{
					pstm.setString(3, rs.getString("attributevalue"));
				}
				
				pstm.addBatch();
				if(++count % batchSize == 0) {
					pstm.executeBatch();
				}
			}
			pstm.executeBatch();
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
	protected void InsertProgramStageInstance(Connection cnn) throws Exception{
		try{
			query = "CREATE TEMP SEQUENCE IF NOT EXISTS temp_programstageinstance_seq INCREMENT BY 1 START 1;";
			pstm  = cnn.prepareStatement(query);
			pstm.execute();
			
			query  = "CREATE TEMP TABLE temp_programstageinstance (";
			query += "programstageinstanceid integer NOT NULL DEFAULT nextval('temp_programstageinstance_seq'),";
			query += "programinstanceid integer NOT NULL,";
			query += "programstageid integer NOT NULL,";
			query += "duedate timestamp without time zone,";
			query += "executiondate timestamp without time zone,";
			query += "organisationunitid integer,";
			query += "status character varying(25),";
			query += "completeduser character varying(255),";
			query += "completeddate timestamp without time zone,";
			query += "uid character varying(11),";
			query += "CONSTRAINT programstageinstance_pkey PRIMARY KEY (programstageinstanceid)";
			query += ");";
			pstm   = cnn.prepareStatement(query);
			pstm.execute();
			
			query = "SELECT setval('temp_programstageinstance_seq', COALESCE((SELECT MAX(programstageinstanceid)+1 FROM programstageinstance), 1), false);";
			pstm   = cnn.prepareStatement(query);
			pstm.execute();
			
			query  = "insert into temp_programstageinstance ";
			query += "(programinstanceid,programstageid,duedate,executiondate,organisationunitid,status,completeduser,completeddate,uid)";
			query += "select distinct on (t1.psi) ";
			query += "t2.programinstanceid, t3.programstageid,t1.executiondate as duedate,t1.executiondate,t4.organisationunitid,";
			query += "'COMPLETED' as status,'"+duser+"' as completeduser,now()::timestamp(3) as completeddate,t1.psi ";
			query += "from "+tablename+" t1 ";
			query += "left join programinstance t2 on t1.pi = t2.uid ";
			query += "left join programstage t3 on t1.ps = t3.uid    ";
			query += "left join organisationunit t4 on t1.ou = t4.uid; ";
			pstm   = cnn.prepareStatement(query);
			pstm.execute();
			
			query  = "insert into programstageinstance(programstageinstanceid,programinstanceid,programstageid,duedate,";
			query += "executiondate,organisationunitid,status,completeddate,uid,created,lastupdated)";
			query += "select programstageinstanceid,";
			query += "programinstanceid,programstageid,duedate,executiondate,organisationunitid,status,completeddate,uid,";
			query += "now()::timestamp(3) as created,now()::timestamp(3) as updated ";
			query += "from temp_programstageinstance;";
			pstm   = cnn.prepareStatement(query);
			pstm.execute();
			cnn.commit();
			System.out.println("Program stage instance insert completed");
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected void InsertTrackedEntityDataValue(Connection cnn) throws Exception{
		try{
			query  = "select distinct on (t1.programstageinstanceid, t2.dataelementid) ";
			query += "t1.programstageinstanceid,t2.dataelementid,t1.dataelementvalue,";
			query += "FALSE as providedelsewhere, ";
			query += "now()::timestamp(3) as created,now()::timestamp(3) as lastupdated ";
			query += "from (";
			query += "select psi,programstageinstanceid,";
			query += "unnest(array"+RemoveUnwanted(columns).toString().replace("\"", "'")+") as dataelement,";
			query += "unnest(array"+RemoveUnwanted(columns).toString().replace("\"", "")+") as dataelementvalue ";
			query += "from "+tablename+" t1 ";
			query += "left join programstageinstance t2 on t1.psi = t2.uid";
			query += ") t1 ";
			query += "left join dataelement t2 on t1.dataelement ilike t2.uid ";
			query += "where t1.dataelementvalue is not null and t2.dataelementid is not null ";
			
			 
			
			pstm  = cnn.prepareStatement(query);
			ResultSet rs = pstm.executeQuery();
			
			//Stored the data into result set
			//then loop inside to clean/convert java dates into sql dates
			
			query = "insert into trackedentitydatavalue values (?,?,?,?,?,?);";
			pstm = cnn.prepareStatement(query);
			
			final int batchSize = 1000;
			int count=0;
			
			while(rs.next()){
				pstm.setInt(1, rs.getInt("programstageinstanceid"));
				pstm.setInt(2, rs.getInt("dataelementid"));
				
				//Detect date values
				if(rs.getString("dataelementvalue").length() == 7 || rs.getString("dataelementvalue").length() == 6){
					pstm.setString(3, SQLDate(rs.getString("dataelementvalue")));
				}else{
					pstm.setString(3, rs.getString("dataelementvalue"));
				}
				
				pstm.setBoolean(4, rs.getBoolean("providedelsewhere"));
				pstm.setDate(5, rs.getDate("created"));
				pstm.setDate(6, rs.getDate("lastupdated"));
				
				pstm.addBatch();
				if(++count % batchSize == 0) {
					pstm.executeBatch();
				}
			}
			pstm.executeBatch();
			cnn.commit();
			System.out.println("Trackedentitydatavalue insert completed");
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
	protected void DropAnalyticsTable(Connection cnn) throws Exception{
		//Finalizing migration by deleting the temporary table
		try{
			System.out.print("remove created table " + tablename + "...");
			query = "DROP TABLE IF EXISTS "+tablename+";";
			pstm = cnn.prepareStatement(query);
			System.out.println(pstm.toString());
			pstm.execute();
			cnn.commit();
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected String SQLDate(String sdate) throws Exception{
		Date dateParsed;
		java.sql.Date dateSql;
		
		try{
			dateParsed = formatter.parse(sdate);
			dateSql = new java.sql.Date(dateParsed.getTime());
		}catch(Exception e1){
			try{
				formatter = new SimpleDateFormat("dd/mm/yy",Locale.ENGLISH);
				dateParsed = formatter.parse(sdate);
				dateSql = new java.sql.Date(dateParsed.getTime());
			}catch(Exception e2){
				try{
					formatter = new SimpleDateFormat("yyyy-mm-dd",Locale.ENGLISH);
					dateParsed = formatter.parse(sdate);
					dateSql = new java.sql.Date(dateParsed.getTime());
				}catch(Exception e3){
					return sdate;
				}
			}
		}
		//Take only the first 10 characters 12:12:12 00
		return dateSql.toString().substring(0, 10);
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
	protected JSONArray RemoveUnwanted(JSONArray jarr){
		//all the UUID are 11 size by length
		JSONArray j = new JSONArray();
		for(int i=0;i<jarr.length();i++){
			if(jarr.getString(i).length() == 11){
				j.put(jarr.getString(i));
			}
		}
		return j;
	}

}
