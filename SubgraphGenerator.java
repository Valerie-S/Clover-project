package generate;

/**
 * An example to generate .sgrf file including: Directory reader, Individual file reader, Reformat, and DBOutputTable
 * Wrapping all components into a subgraph
 **/

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SubgraphGenerator {
	public static void main(String args[]) throws IOException {
		String name = "fund";
		String prefix = "fnd";
		String client_code = "IAC";

		File file = new File("C:/Users/if994248/Workspaces/my_workspace/xml-generator/temp/" + name + "-subgraph-v.sgrf");
		if(!file.exists()) {
			file.createNewFile();
		}
		System.out.println("File created successfully");
		FileWriter fw = new FileWriter(file);

		BufferedWriter bw = new BufferedWriter(fw);

		bw.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		bw.write("<Graph author=\"guest179\" created=\"Fri May 10 09:46:12 EDT 2019\" guiVersion=\"4.6.0\" id=\"1557506726243\" licenseCode=\"CLP1DIFDSG23416366EX\" name=\"" + name + "-subgraph-v\" nature=\"subgraph\" showComponentDetails=\"true\">\n");
		bw.write("<Global>\n");
		
		Connection ta_ods = null;
		Connection idr_iac = null;
		Statement stmt = null;
		
		bw.write("<Metadata id=\"Metadata0\">\n");
		bw.write("<Record fieldDelimiter=\";\" name=\"" + name + "_from_file\" previewAttachmentCharset=\"UTF-8\" recordDelimiter=\"\\n\" type=\"delimited\">\n");
		try {
			Class.forName("org.postgresql.Driver");
			idr_iac = DriverManager.getConnection("jdbc:mysql://localhost:3306/valerie_idr", "root", "admin");
			ta_ods = DriverManager.getConnection("jdbc:postgresql://localhost:5432/new_ta_ods", "if994248", "password");
			System.out.println("Opened database successfully!");

			// Metadata from_idr ---------------------------------------------------------------------------------
			String n = "select * from information_schema.columns where table_name = \"" + name + "\" and table_schema = \"valerie_idr\" order by ORDINAL_POSITION asc;";
			stmt = idr_iac.createStatement();
			ResultSet set = stmt.executeQuery(n);
			while(set.next()) {
				String column_name = set.getString("column_name").toLowerCase();
				String type = set.getString("data_type");
				if(type.equalsIgnoreCase("char")) {
					bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("character_maximum_length") + "\" type=\"string\"/>\n");
				} else if(type.equalsIgnoreCase("bigint")) {
					bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("numeric_precision") + "\" type=\"long\"/>\n");
				} else if(type.equalsIgnoreCase("int")) {
					bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("numeric_precision") + "\" type=\"integer\"/>\n");
				} else if(type.equalsIgnoreCase("decimal")) {
					bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("numeric_precision") + "\" type=\"decimal\"/>\n");
				} else if(type.equalsIgnoreCase("date")) {
					//date default length is 13
					bw.write("<Field format=\"yyyy-MM-dd\" name=\"" + column_name + "\" size=\"13\" type=\"date\"/>\n");
				} else {
					bw.write("<Field name=\"" + column_name + "\" size=\"" + set.getString("character_maximun_lengths") + "\" type=\"" + type + "\"/>\n");
				}
			}
			bw.write("<Field name=\"empty\" type=\"string\"/>\n");
			bw.write("</Record>\n");
			bw.write("</Metadata>\n");
			
			// Metadata to_ta_ods  -------------------------------------------------------------------------------------
			bw.write("<Metadata id=\"Metadata1\">\n");
			bw.write("<Record fieldDelimiter=\";\" name=\"" + name + "_to_ta_ods\" previewAttachmentCharset=\"UTF-8\" recordDelimiter=\"\\n\" type=\"delimited\">\n");
			String select_ta_ods = "select concat('<Field name=\"',column_name,'\" size=\"', character_maximum_length,'\" type=\"', "
					+ "case when data_type = 'character varying' then 'string' \r\n" + 
					"		when data_type = 'bigint' then 'long' \r\n" + 
					"		when data_type = 'numeric' then 'decimal' \r\n" + 
					"  else data_type end,'\"/>') from information_schema.\"columns\" where table_name = '"+ name + "' and table_schema = 'valerie_ta_ods';";
			stmt = ta_ods.createStatement();
			ResultSet tdb = stmt.executeQuery(select_ta_ods);
			while(tdb.next()) {
				String line = tdb.getString("concat");
				bw.write(line + "\n");
				//System.out.println(line);
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
			bw.write("<GraphParameter name=\"domain_prefix\" public=\"false\" value=\"" + prefix + "\"/>\n");
			bw.write("<GraphParameter name=\"env_code\" public=\"false\" value=\"DEVSLOTX374\"/>\n");
			bw.write("<GraphParameterFile fileURL=\"workspace.prm\"/>\n");
			bw.write("</GraphParameters>\n");
			bw.write("<RichTextNote backgroundColor=\"FAF6D6\" folded=\"false\" fontSize=\"medium\" height=\"500\" id=\"Note0\" textColor=\"444444\" width=\"1200\" x=\"50\" y=\"50\">\n");
			bw.write("<attr name=\"text\"><![CDATA[h3. Load " + name + " table: " + prefix + "]]></attr>\n");
			bw.write("</RichTextNote>\n");
			bw.write("<Dictionary/>\n");
			bw.write("</Global>\n");
			bw.write("<Phase number=\"0\">\n");
			
			// Graph components -------------------------------------------------------------------------
			bw.write("<Node FileUrl=\"${DATAIN_DIR}/\" genericTransformClass=\"com.ifdsgroup.idr.IDRCustomDirectoryInsert\" guiName=\"CustomDirectoryReader\" guiX=\"184\" guiY=\"177\" id=\"CUSTOM_DIRECTORY_READER\" type=\"GENERIC_READER\"/>\n");
			bw.write("<Node FileUrl=\"${DATAIN_DIR}/\" genericTransformClass=\"com.ifdsgroup.idr.UnivocityMultipleInsertReader\" guiName=\"Custom" + name.substring(0, 1).toUpperCase() + name.substring(1) + "\" guiX=\"359\" guiY=\"173\" id=\"CUSTOM_"+ name.toUpperCase() + "\" type=\"GENERIC_READER\"/>\n");
			/**  DBoutputTable   */
			String select_db_col = "select array_to_string(array_agg(concat('\"', column_name, '\"')), ', ') as db_columns from information_schema.\"columns\" where table_name = '" + name + "' and table_schema='valerie_ta_ods';";
			bw.write("<Node dbConnection=\"JDBC0\" guiName=\"Load" + name.substring(0, 1).toUpperCase() + name.substring(1) + "\" guiX=\"849\" guiY=\"173\" id=\"LOAD_" + name.toUpperCase() + "\" type=\"DB_OUTPUT_TABLE\">\n");
			ResultSet db_columns = stmt.executeQuery(select_db_col);
			if(db_columns.next()) {			// required to use next() even ResultSet has only one row
				bw.write("<attr name=\"sqlQuery\"><![CDATA[INSERT INTO \"valerie_ta_ods\".\"" + name + "\" (" + db_columns.getString("db_columns") + ")\n");
			}
			String select_clover_col = "select array_to_string(array_agg(concat('$', column_name)), ', ') as clover_columns from information_schema.\"columns\" where table_name = '" + name + "' and table_schema='valerie_ta_ods';";
			ResultSet clover_columns = stmt.executeQuery(select_clover_col);
			if(clover_columns.next()) {
				bw.write("VALUES (" + clover_columns.getString("clover_columns") + ")]]></attr>\n");
			}
			bw.write("</Node>");
			/**  Reformat   */
			bw.write("<Node guiName=\"Reformat\" guiX=\"742\" guiY=\"145\" id=\"REFORMAT\" type=\"REFORMAT\">\n");
			bw.write("<attr name=\"transform\"><![CDATA[//#CTL2\n\n");
			bw.write("// Transforms input record into output record.\n");
			bw.write("function integer transform() {\n");
			bw.write("	$out.0.client_code = \"${client_code}\";\n");
			String generate_transform = "select concat('$out.0.', column_name, ' = $in.0.', column_name, ';') as mapping from information_schema.\"columns\" where table_name = '" + name + "' and column_name not in('client_code');";
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
			bw.write("<Node guiName=\"SubgraphInput\" guiX=\"50\" guiY=\"10\" id=\"SUBGRAPH_INPUT\" type=\"SUBGRAPH_INPUT\">\r\n" + 
					"<Port guiY=\"110\" name=\"0\"/>\r\n" + 
					"</Node>\r\n" + 
					"<Node guiName=\"SubgraphOutput\" guiX=\"1221\" guiY=\"10\" id=\"SUBGRAPH_OUTPUT\" type=\"SUBGRAPH_OUTPUT\">\r\n" + 
					"<Port guiY=\"110\" name=\"0\"/>\r\n" + 
					"</Node>");
			
			// Edge ------------------------------------
			bw.write("<Edge fromNode=\"CUSTOM_" + name.toUpperCase() + ":0\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge0\" inPort=\"Port 0 (in)\" metadata=\"Metadata0\" outPort=\"Port 0 (out)\" toNode=\"REFORMAT:0\"/>\n");
			bw.write("<Edge fromNode=\"CUSTOM_DIRECTORY_READER:0\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge3\" inPort=\"Port 0 (in)\" metadata=\"Metadata2\" outPort=\"Port 0 (out)\" toNode=\"CUSTOM_" + name.toUpperCase() + ":0\"/>\n");
			bw.write("<Edge fromNode=\"REFORMAT:0\" guiBendpoints=\"\" guiRouter=\"Manhattan\" id=\"Edge1\" inPort=\"Port 0 (in)\" metadata=\"Metadata1\" outPort=\"Port 0 (out)\" toNode=\"LOAD_" + name.toUpperCase() + ":0\"/>\n");
			
			bw.write("</Phase>\n");
			bw.write("</Graph>\n");
			
			stmt.close();
			idr_iac.close();
			ta_ods.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Record created successfully");

		try {
			if(bw != null) bw.close();
			if(fw != null) fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



}
