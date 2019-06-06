package generate;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

public class MainGraph {

	private static Connection ta_ods;
	private static Connection idr_iac;
	private static Statement statement = null;
	private static String client_code = "IAC";
	private static String table_schema = "valerie_ta_ods";

	public static void main(String args[]) throws ClassNotFoundException, SQLException, IOException {

		Class.forName("org.postgresql.Driver");
		ta_ods = DriverManager.getConnection("jdbc:postgresql://localhost:5432/new_ta_ods", "if994248", "password");
		idr_iac = DriverManager.getConnection("jdbc:mysql://localhost:3306/valerie_idr", "root", "admin");

		HashMap<Integer, String[]> mapping = getTables("idr_iac");

		File file = new File("C:/Users/if994248/Workspaces/my_workspace/clover-graph-generator/graph/main-graph.grf");
		if(!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file);
		BufferedWriter bw = new BufferedWriter(fw);

		bw.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		bw.write("<Graph author=\"if994248\" created=\"Wed May 29 15:26:47 EDT 2019\" guiVersion=\"4.6.0\" id=\"1559313877539\" licenseCode=\"RegCode\" name=\"main-graph\" showComponentDetails=\"true\">\n");
		bw.write("<Global>\n");
		bw.write("<Metadata id=\"Metadata0\">\n");
		bw.write("<Record fieldDelimiter=\";\" name=\"file_url\" recordDelimiter=\"\\n\" type=\"delimited\">\n");
		bw.write("<Field name=\"fileName\" type=\"string\"/>\n");
		bw.write("</Record>\n");
		bw.write("</Metadata>\n");
		bw.write("<GraphParameters>\n");
		bw.write("<GraphParameter name=\"client_code\" public=\"true\" value=\"IAC\"/>\n");
		bw.write("<GraphParameterFile fileURL=\"workspace.prm\"/>\n");
		bw.write("</GraphParameters>\n");
		bw.write("<Dictionary/>\n");
		bw.write("</Global>\n");

		bw.write("<Phase number=\"0\">\n");
		
		int guiY = 100;
		for(int key = 1; mapping.containsKey(key); key ++) {
			
			String table_name = mapping.get(key)[0];
			String domain_prefix = mapping.get(key)[1];
			bw.write("<Node guiName=\"" + table_name + "-subgraph\" guiX=\"706\" guiY=\"" + guiY + "\" id=\"" + table_name.toUpperCase() + "_SUBGRAPH\" jobURL=\"${SUBGRAPH_DIR}/" + table_name + "-subgraph.sgrf\" type=\"SUBGRAPH\"/>\n");
			Subgraph subgraph = new Subgraph();
			subgraph.createSubgraph(client_code, table_name, table_schema, domain_prefix);
			
			guiY += 100;
			
		}
		bw.write("<Node genericTransformClass=\"com.ifdsgroup.idr.IDRCustomDirectoryInsert\" guiName=\"CustomDirectoryReader\" guiX=\"206\" guiY=\"256\" id=\"CUSTOM_DIRECTORY_READER\" passThroughOutputPort=\"1\" type=\"GENERIC_READER\">\n");
		bw.write("<attr name=\"FileUrl\"><![CDATA[${DATAIN_DIR}/]]></attr>\n");
		bw.write("</Node>\n");
		bw.write("<Node guiName=\"SimpleCopy\" guiX=\"395\" guiY=\"256\" id=\"SIMPLE_COPY\" type=\"SIMPLE_COPY\"/>\n");
		bw.write("<Edge fromNode=\"CUSTOM_DIRECTORY_READER:0\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge0\" inPort=\"Port 0 (in)\" metadata=\"Metadata0\" outPort=\"Port 0 (out)\" toNode=\"SIMPLE_COPY:0\"/>\n");
		
		int port = 0;
		for(int key = 1; mapping.containsKey(key); key ++) {
			String table_name = mapping.get(key)[0];
			bw.write("<Edge fromNode=\"SIMPLE_COPY:" + port + "\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge" + key + "\" inPort=\"Port 0 (in)\" metadata=\"Metadata0\" outPort=\"Port " + port + " (out)\" toNode=\"" + table_name.toUpperCase() + "_SUBGRAPH:0\"/>\n");
			port ++;
		}
		bw.write("</Phase>\n");
		bw.write("</Graph>\n");
		bw.write("\n");

		if(bw != null) bw.close();
		if(fw != null) fw.close();
	}

	private static HashMap<Integer, String[]> getTables(String table_schema) throws SQLException {
		HashMap<Integer, String[]> mapping = new HashMap<Integer, String[]>();     // key, {table_name, domestic_prefix}
		// SQL Query selecting from the given table_schema
		String sql = "select filename, tablename from " + table_schema + ".filetablemap order by filename asc;";
		//statement = ta_ods.createStatement();
		statement = idr_iac.createStatement();
		ResultSet getMapping = statement.executeQuery(sql);
		
		int key = 1;
		while(getMapping.next()) {
			String[] array = new String[2];
			array[0] = getMapping.getString("tablename").toLowerCase();
			array[1] = getMapping.getString("filename").toLowerCase();
			mapping.put(key, array);
			System.out.println(mapping.get(key)[0]);
			key++;
		}
		
		return mapping;
	}

}
