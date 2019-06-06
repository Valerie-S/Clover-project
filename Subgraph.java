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

/**
 * Generates customized subgraphs from main-graph
 * Includes: Individual File Reader, Reformat, DBOutputTable
 */

public class Subgraph {
	
	private String idr_schema = "valerie_idr";
	
	public void createSubgraph(String client_code, String table_name, String table_schema, String domain_prefix) throws IOException, SQLException, ClassNotFoundException {

		File file = new File("C:/Users/if994248/Workspaces/my_workspace/clover-graph-generator/graph/subgraph/" + table_name + "-subgraph.sgrf");
		if(!file.exists()) {
			file.createNewFile();
		}
		//System.out.println("File created successfully");
		FileWriter fw = new FileWriter(file);

		BufferedWriter bw = new BufferedWriter(fw);

		bw.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		bw.write("<Graph author=\"guest179\" created=\"Fri May 10 09:46:12 EDT 2019\" guiVersion=\"4.6.0\" id=\"1557506726243\" licenseCode=\"CLP1DIFDSG23416366EX\" name=\"" + table_name + "-subgraph-v\" nature=\"subgraph\" showComponentDetails=\"true\">\n");
		bw.write("<Global>\n");

		Connection ta_ods = null;
		Connection idr_iac = null;
		Statement stmt = null;

		Class.forName("org.postgresql.Driver");
		idr_iac = DriverManager.getConnection("jdbc:mysql://localhost:3306/valerie_idr", "root", "admin");
		ta_ods = DriverManager.getConnection("jdbc:postgresql://localhost:5432/new_ta_ods", "if994248", "password");

		// Metadata from_idr ---------------------------------------------------------------------------------
		bw.write("<Metadata id=\"Metadata0\">\n");
		bw.write("<Record fieldDelimiter=\";\" name=\"" + table_name + "_from_file\" previewAttachmentCharset=\"UTF-8\" recordDelimiter=\"\\n\" type=\"delimited\">\n");
		String n = "select * from information_schema.columns where table_name = \"" + table_name + "\" and table_schema = \"" + idr_schema + "\" order by ORDINAL_POSITION asc;";
		stmt = idr_iac.createStatement();
		ResultSet set = stmt.executeQuery(n);
		while(set.next()) {
			String column_name = set.getString("column_name").toLowerCase();
			String type = set.getString("data_type");
			if(type.equalsIgnoreCase("char") || type.equalsIgnoreCase("varchar")) {
				bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("character_maximum_length") + "\" type=\"string\"/>\n");
			} else if(type.equalsIgnoreCase("bigint")) {
				bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("numeric_precision") + "\" type=\"long\"/>\n");
			} else if(type.equalsIgnoreCase("int") || type.equalsIgnoreCase("tinyint")) {
				bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("numeric_precision") + "\" type=\"integer\"/>\n");
			} else if(type.equalsIgnoreCase("decimal")) {
				bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("numeric_precision") + "\" type=\"decimal\"/>\n");
			} else if(type.equalsIgnoreCase("date")) {
				//date default length is 13
				bw.write("<Field format=\"yyyy-MM-dd\" name=\"" + column_name + "\" size=\"13\" type=\"date\"/>\n");
			} else if(type.equalsIgnoreCase("datetime") || type.equalsIgnoreCase("timestamp")) {
				bw.write("<Field format=\"yyyy-MM-dd-HH.mm.ss\" name=\"" + column_name + "\" size=\"23\" type=\"date\"/>\n");
			} else {
				bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("character_maximum_length") + "\" type=\"" + type + "\"/>\n");
			}
		}
		bw.write("<Field name=\"empty\" type=\"string\"/>\n");
		bw.write("</Record>\n");
		bw.write("</Metadata>\n");

		// Metadata to_ta_ods  -------------------------------------------------------------------------------------
		bw.write("<Metadata id=\"Metadata1\">\n");
		bw.write("<Record fieldDelimiter=\";\" name=\"" + table_name + "_to_ta_ods\" previewAttachmentCharset=\"UTF-8\" recordDelimiter=\"\\n\" type=\"delimited\">\n");
		String select_ta_ods = "select * from information_schema.\"columns\" where table_name = '" + table_name + "' and table_schema = '" + table_schema + "' order by ordinal_position asc;";
		stmt = ta_ods.createStatement();
		ResultSet tdb = stmt.executeQuery(select_ta_ods);
		while(tdb.next()) {
			String column_name = tdb.getString("column_name");
			String type = tdb.getString("data_type");
			if(type.equalsIgnoreCase("character varying")) {
				bw.write("<Field name=\"" + column_name + "\" size=\"" + tdb.getString("character_maximum_length") + "\" type=\"string\"/>\n");
			} else if(type.equalsIgnoreCase("bigint")) {
				bw.write("<Field name=\"" + column_name + "\" size=\"19\" type=\"long\"/>\n");
			} else if(type.equalsIgnoreCase("numeric")) {
				bw.write("<Field name=\"" + column_name + "\" size=\"" + tdb.getString("numeric_precision") + "\" type=\"decimal\"/>\n");
			} else if(type.equalsIgnoreCase("integer") || type.equalsIgnoreCase("smallint")) {
				bw.write("<Field name=\"" + column_name + "\" size=\"10\" type=\"integer\"/>\n");
			} else if(type.equalsIgnoreCase("smallint")){
				bw.write("<Field name=\"" + column_name + "\" size=\"3\" type=\"integer\"/>\n");
			} else if(type.equalsIgnoreCase("date")) {
				bw.write("<Field format=\"yyyy-MM-dd\" name=\"" + column_name + "\" size=\"13\" type=\"date\"/>\n");
			} else if(type.equalsIgnoreCase("timestamp without time zone") || type.equalsIgnoreCase("timestamp")) {
				bw.write("<Field format=\"yyyy-MM-dd-HH.mm.ss\" name=\"" + column_name + "\" size=\"23\" type=\"date\"/>\n");
			} else {
				bw.write("<Field name=\"" + column_name + "\" size=\"" + tdb.getString("character_maximum_length") + "\" type=\"" + type + "\"/>\n");
			}
		}
		bw.write("</Record>\n");
		bw.write("</Metadata>\n");

		// Metadata for Directory Reader ------------------------------------------------------------------------
		bw.write("<Metadata id=\"Metadata2\">\n");
		bw.write("<Record fieldDelimiter=\";\" name=\"file_url\" previewAttachmentCharset=\"UTF-8\" recordDelimiter=\"\\n\" type=\"delimited\">\n");
		bw.write("<Field name=\"fileName\" type=\"string\"/>\n");
		bw.write("</Record>\n");
		bw.write("</Metadata>\n");

		// Connection, parameters, note ------------------------------------------------------------------------
		bw.write("<Connection database=\"POSTGRE\" dbURL=\"jdbc:postgresql://localhost:5432/new_ta_ods\" id=\"JDBC0\" jdbcSpecific=\"POSTGRE\" name=\"new_ta_ods\" password=\"password\" type=\"JDBC\" user=\"if994248\"/>\n");
		bw.write("<GraphParameters>\n");
		bw.write("<GraphParameter name=\"client_code\" public=\"true\" value=\"" + client_code + "\"/>\n");
		bw.write("<GraphParameter name=\"domain_prefix\" public=\"false\" value=\"" + domain_prefix + "\"/>\n");
		bw.write("<GraphParameter name=\"env_code\" public=\"false\" value=\"DEVSLOTX374\"/>\n");
		bw.write("<GraphParameterFile fileURL=\"workspace.prm\"/>\n");
		bw.write("</GraphParameters>\n");
		bw.write("<RichTextNote backgroundColor=\"FAF6D6\" folded=\"false\" fontSize=\"medium\" height=\"300\" id=\"Note0\" textColor=\"444444\" width=\"900\" x=\"300\" y=\"50\">\n");
		bw.write("<attr name=\"text\"><![CDATA[h3. Subgraph " + table_name + ": " + domain_prefix + "]]></attr>\n");
		bw.write("</RichTextNote>\n");
		bw.write("<Dictionary/>\n");
		bw.write("</Global>\n");
		
		bw.write("<Phase number=\"0\">\n");

		// Graph components -------------------------------------------------------------------------
		// Directory Reader for subgraph debug
		bw.write("<Node debugInput=\"true\" genericTransformClass=\"com.ifdsgroup.idr.IDRCustomDirectoryInsert\" guiName=\"CustomDirectoryReader\" guiX=\"46\" guiY=\"133\" id=\"CUSTOM_DIRECTORY_READER\" type=\"GENERIC_READER\">\n");
		bw.write("<attr name=\"FileUrl\"><![CDATA[${DATAIN_DIR}/]]></attr>\n");
		bw.write("</Node>");
		// Individual file reader
		bw.write("<Node FileUrl=\"${DATAIN_DIR}/\" genericTransformClass=\"com.ifdsgroup.idr.UnivocityMultipleInsertReader\" guiName=\"Read" + table_name.substring(0, 1).toUpperCase() + table_name.substring(1) + "\" guiX=\"392\" guiY=\"145\" id=\"READ_"+ table_name.toUpperCase() + "\" type=\"GENERIC_READER\"/>\n");
		// Writer
		String select_db_col = "select array_to_string(array_agg(concat('\"', column_name, '\"')), ', ') as db_columns from information_schema.\"columns\" where table_name = '" + table_name + "' and table_schema='" + table_schema + "';";
		bw.write("<Node dbConnection=\"JDBC0\" guiName=\"Load" + table_name.substring(0, 1).toUpperCase() + table_name.substring(1) + "\" guiX=\"980\" guiY=\"145\" id=\"LOAD_" + table_name.toUpperCase() + "\" type=\"DB_OUTPUT_TABLE\">\n");
		ResultSet db_columns = stmt.executeQuery(select_db_col);
		if(db_columns.next()) {			// required to use next() even ResultSet has only one row
			bw.write("<attr name=\"sqlQuery\"><![CDATA[INSERT INTO \"" + table_schema + "\".\"" + table_name + "\" (" + db_columns.getString("db_columns") + ")\n");
		}
		String select_clover_col = "select array_to_string(array_agg(concat('$', column_name)), ', ') as clover_columns from information_schema.\"columns\" where table_name = '" + table_name + "' and table_schema='" + table_schema + "';";
		String select_prk = "select array_to_string(array_agg(concat(column_name)), ', ') as prk from information_schema.\"constraint_column_usage\" where  table_name = '" + table_name + "' and table_schema = '" + table_schema + "';";
		String concat_non_prk = "select array_to_string(array_agg(concat(column_name, ' = excluded.', column_name)), ', ') as non_prk from information_schema.\"columns\" \r\n" + 
								"	where table_name = '" + table_name + "' and table_schema = '" + table_schema + "' and column_name not in (\r\n" + 
								"	select column_name from information_schema.\"constraint_column_usage\" \r\n" + 
								"		where table_name = '" + table_name + "' and table_schema = '" + table_schema + "');";
		ResultSet clover_columns = stmt.executeQuery(select_clover_col);
		if(clover_columns.next()) {
			bw.write("VALUES (" + clover_columns.getString("clover_columns"));
		}
		ResultSet primary_key = stmt.executeQuery(select_prk);
		if(primary_key.next()) {
			bw.write(") on conflict(" + primary_key.getString("prk"));
		}
		ResultSet non_primary = stmt.executeQuery(concat_non_prk);
		if(non_primary.next()) {
			bw.write(") do update set " + non_primary.getString("non_prk") +"]]></attr>\n");
		}
		bw.write("</Node>");
		// Reformat
		bw.write("<Node guiName=\"Reformat\" guiX=\"610\" guiY=\"145\" id=\"REFORMAT\" type=\"REFORMAT\">\n");
		bw.write("<attr name=\"transform\"><![CDATA[//#CTL2\n\n");
		bw.write("// Transforms input record into output record.\n");
		bw.write("function integer transform() {\n");
		bw.write("	$out.0.client_code = \"${client_code}\";\n");
		String generate_transform = "select concat('$out.0.', column_name, ' = $in.0.', column_name, ';') as mapping from information_schema.\"columns\" where table_name = '" + table_name + "' and column_name not in('client_code');";
		ResultSet mapping = stmt.executeQuery(generate_transform);
		while(mapping.next()) {
			String line = mapping.getString("mapping");
			bw.write("	" + line + "\n");
		}
		bw.write("\n	return ALL;\n}\n");
		bw.write("// Called during component initialization.\r\n" + 
				"// function boolean init() {}\r\n" + 
				"\r\n" + 
				"// Called during each graph run before the transform is executed. May be used to allocate and initialize resources\r\n" + 
				"// required by the transform. All resources allocated within this method should be released\r\n" + 
				"// by the postExecute() method.\r\n" + 
				"// function void preExecute() {}\r\n" + 
				"\r\n" + 
				"// Called only if transform() throws an exception.\r\n" + 
				"// function integer transformOnError(string errorMessage, string stackTrace) {}\r\n" + 
				"\r\n" + 
				"// Called during each graph run after the entire transform was executed. Should be used to free any resources\r\n" + 
				"// allocated within the preExecute() method.\r\n" + 
				"// function void postExecute() {}\r\n" + 
				"\r\n" + 
				"// Called to return a user-defined error message when an error occurs.\r\n" + 
				"// function string getMessage() {}\r\n" + 
				"]]></attr>");
		bw.write("</Node>\n");

		// Subgraph nodes --------------------------------------------------------------------------
		bw.write("<Node guiName=\"SubgraphInput\" guiX=\"228\" guiY=\"10\" id=\"SUBGRAPH_INPUT\" type=\"SUBGRAPH_INPUT\">\r\n" + 
				"<Port guiY=\"162\" name=\"0\"/>\r\n" + 
				"<Port guiY=\"232\" name=\"1\"/>\n" + 
				"</Node>\r\n" + 
				"<Node guiName=\"SubgraphOutput\" guiX=\"1232\" guiY=\"10\" id=\"SUBGRAPH_OUTPUT\" type=\"SUBGRAPH_OUTPUT\">\r\n" + 
				"<Port guiY=\"161\" name=\"0\"/>\r\n" + 
				"</Node>");

		// Edges ------------------------------------
		bw.write("<Edge fromNode=\"CUSTOM_DIRECTORY_READER:0\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge0\" inPort=\"Port 0 (in)\" metadata=\"Metadata2\" outPort=\"Port 0 (out)\" toNode=\"SUBGRAPH_INPUT:0\"/>\n");
		bw.write("<Edge fromNode=\"READ_" + table_name.toUpperCase() + ":0\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge2\" inPort=\"Port 0 (in)\" metadata=\"Metadata0\" outPort=\"Port 0 (out)\" toNode=\"REFORMAT:0\"/>\n");
		bw.write("<Edge fromNode=\"REFORMAT:0\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge3\" inPort=\"Port 0 (in)\" metadata=\"Metadata1\" outPort=\"Port 0 (out)\" toNode=\"LOAD_" + table_name.toUpperCase() + ":0\"/>\n");
		bw.write("<Edge fromNode=\"SUBGRAPH_INPUT:0\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge1\" inPort=\"Port 0 (in)\" metadata=\"Metadata2\" outPort=\"Port 0 (out)\" toNode=\"READ_" + table_name.toUpperCase() + ":0\"/>");

		bw.write("</Phase>\n");
		bw.write("</Graph>\n");

		stmt.close();
		idr_iac.close();
		ta_ods.close();

		//System.out.println("Record created successfully");

		if(bw != null) bw.close();
		if(fw != null) fw.close();

	}
}
