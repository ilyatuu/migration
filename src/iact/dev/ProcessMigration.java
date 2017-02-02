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

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

/**
* Servlet implementation class ProcessMigration
* 
* DHIS2 JPHIEGO Data Migration Script
* This script will work in reverse order following the delete script (01 - 10), 
* It will start with item No. 10 going backward
* 
* 01. delete from trackedentitydatavalueaudit;
* 02. delete from trackedentitydatavalue;
* 03. delete from programstageinstancecomments;
* 04. delete from programstageinstance;
* 05. delete from programinstancecomments;
* 06. delete from programinstance;
* 07. delete from trackedentityaudit;
* 08. delete from trackedentityattributevalueaudit;
* 09. delete from trackedentityattributevalue;
* 10. delete from trackedentityinstance;
* 
**/

@WebServlet("/ProcessMigration")
public class ProcessMigration extends HttpServlet {
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
    public ProcessMigration() {
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
		duser = request.getParameter("duser");
		programid = Integer.parseInt( request.getParameter("program") );
		columns   = new JSONArray(request.getParameter("columns"));
		jdata	= new JSONArray(request.getParameter("data"));
		
		System.out.println("Program id is " + programid);
		jReturn = new JSONObject();
		try{
			//1.1 Create temporary sequence for tracked instance id
			db = new DbConnect();
			cnn = db.getConn();
			cnn.setAutoCommit(false);
			System.out.println("Data migration started");
			CreateAnalyticsTable(tablename,columns,cnn);
			InsertIntoAnalyticsTable(tablename,columns,cnn);
			Inserttrackedentityinstance(cnn,tablename);
			Inserttrackedentityattributevalue(cnn);
			InsertProgramInstance(cnn, programid);
			InsertProgramStageInstance(cnn,tablename,duser);
			InsertTrackedEntityDataValue(cnn,tablename,columns);
			System.out.println("Data migration completed");
			jReturn.put("success", true);
			jReturn.put("message", "Data migration completed");
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
	
	protected void CreateAnalyticsTable(String tablename, JSONArray columns, Connection cnn) throws Exception {
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
			cnn.commit();
			System.out.println("Analytics table structure created");
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected void InsertIntoAnalyticsTable(String tablename, JSONArray columns, Connection cnn) throws Exception {
		try{
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
			//Print output
			jReturn.put("success", true);
			jReturn.put("message", Integer.toString(jdata.length()) + " records temporary stored");
			//pw.print(jReturn);
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected void Inserttrackedentityinstance(Connection cnn, String tablename) throws Exception{
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
			query  += "left join organisationunit ou on an.ou = ou.uid ";
			query  += "group by tei,organisationunitid;";
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
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected void Inserttrackedentityattributevalue(Connection cnn) throws Exception{
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
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected void InsertProgramInstance(Connection cnn, int progid) throws Exception{
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
			query +="SELECT distinct on (pi) "+ progid + ",";
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
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected void InsertProgramStageInstance(Connection cnn, String tablename, String duser) throws Exception{
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
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
		
	}
	protected void InsertTrackedEntityDataValue(Connection cnn, String tablename, JSONArray columns) throws Exception{
		try{
			query  = "select distinct on (t1.programstageinstanceid, t2.dataelementid) ";
			query += "t1.programstageinstanceid,t2.dataelementid,t1.dataelementvalue,";
			query += "FALSE as providedelsewhere, ";
			query += "now()::timestamp(3) as created,now()::timestamp(3) as lastupdated ";
			query += "from (";
			query += "select psi,programstageinstanceid,";
			query += "unnest(array"+SpliceJSONArray(columns,"psi").toString().replace("\"", "'")+") as dataelement,";
			query += "unnest(array"+SpliceJSONArray(columns,"psi").toString().replace("\"", "")+") as dataelementvalue ";
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
					pstm.setString(3, SQLDate(rs.getString("dataelementvalue")).toString().substring(0, 10));
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
			System.out.println("Finalizing trackedentitydatavalue values insert");
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected void dropAnalyticsTable(Connection cnn, String tablename) throws Exception{
		try{
			query = "DROP TABLE IF EXIST "+tablename;
			pstm = cnn.prepareStatement(query);
			pstm.executeUpdate();
			cnn.commit();
		}catch(Exception e){
			cnn.rollback();
			throw e;
		}
	}
	protected java.sql.Date SQLDate(String sdate) throws Exception{
		Date dateParsed;
		java.sql.Date dateSql;
		try{
			if(sdate.length() == 7 || sdate.length() == 6 ){
				try{
					dateParsed = formatter.parse(sdate);
					dateSql = new java.sql.Date(dateParsed.getTime());
				}catch(Exception e1){
					try{
						formatter = new SimpleDateFormat("dd/mm/yy",Locale.ENGLISH);
						dateParsed = formatter.parse(sdate);
						dateSql = new java.sql.Date(dateParsed.getTime());
					}catch(Exception e2){
						throw e2;
					}
				}
			}else{
				if(sdate.length() == 10){
					formatter = new SimpleDateFormat("yyyy-dd-mm");
					dateParsed = formatter.parse(sdate);
					dateSql = new java.sql.Date(dateParsed.getTime());
				}else{
					System.out.println("wrong input date is "+sdate.length()+" long with value "+sdate);
					return null;
				}
			}
		}catch(Exception e){
			throw e;
		}
		return dateSql;
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
