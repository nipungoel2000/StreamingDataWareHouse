// Refernces for API : https://docs.oracle.com/javase/7/docs/api/org/w3c/dom/Document.html
// java -classpath "/usr/share/java/mysql-connector-java-8.0.28.jar:." ConfigParser
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import java.util.HashMap;
import java.util.ArrayList;

import java.sql.*;
// import java.io.InputStream;

public class ConfigParser {

    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    String JDBC_URL = "jdbc:mysql://localhost/?useSSL=false";
    String username = "root";
    String password = "root";

    Connection connection = null;

    String XMLFILENAME;
    HashMap<String, ArrayList<HashMap<String, String>>> tables;
    // ArrayList<HashMap<String,String>> fact_table;
    HashMap<String, String> table_data_loc;

    // constructor
    public ConfigParser() {
        this.tables = new HashMap<String, ArrayList<HashMap<String, String>>>();
        // this.fact_table = new ArrayList<HashMap<String,String>>();
        this.table_data_loc = new HashMap<String, String>();
    }

    public void parse(String fname) {
        try {
            this.XMLFILENAME = fname;
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(new File(this.XMLFILENAME));
            doc.getDocumentElement().normalize();

            NodeList dim_list = doc.getElementsByTagName("dim");
            for (int i = 0; i < dim_list.getLength(); i++) {
                Node dim_node = dim_list.item(i);
                if (dim_node.getNodeType() == Node.ELEMENT_NODE) {
                    Element dim_element = (Element) dim_node;
                    ArrayList<HashMap<String, String>> fields = new ArrayList<HashMap<String, String>>();
                    // get dimension's name attribute
                    String dim_name = "dim_" + dim_element.getAttribute("name");
                    String data_loc = dim_element.getElementsByTagName("data-loc").item(0).getTextContent();
                    NodeList field_list = dim_element.getElementsByTagName("field");

                    // System.out.println(dim_name);
                    // System.out.println("Length : " + field_list.getLength());
                    // System.out.println("data_loc : " + data_loc);

                    for (int j = 0; j < field_list.getLength(); j++) {
                        HashMap<String, String> field = new HashMap<String, String>();
                        Node field_node = field_list.item(j);
                        if (field_node.getNodeType() == Node.ELEMENT_NODE) {
                            Element field_element = (Element) field_node;
                            String is_pk = field_element.getAttribute("is-pk");
                            String field_name = field_element.getElementsByTagName("name").item(0).getTextContent();
                            String field_type = field_element.getElementsByTagName("type").item(0).getTextContent();
                            field.put("FIELD_NAME", field_name);
                            field.put("FIELD_TYPE", field_type);
                            field.put("PRIMARY_KEY", is_pk);
                            field.put("FOREIGN_KEY", "");
                            fields.add(field);
                            // System.out.println(is_pk + " " + field_name + " " + field_type);
                        }
                    }
                    (this.tables).put(dim_name, fields);
                    (this.table_data_loc).put(dim_name, data_loc);
                }
            }

            Node var_node = doc.getElementsByTagName("variables").item(0);
            if (var_node.getNodeType() == Node.ELEMENT_NODE) {
                Element var_element = (Element) var_node;
                NodeList col_list = var_element.getElementsByTagName("column");
                ArrayList<HashMap<String, String>> columns = new ArrayList<HashMap<String, String>>();
                for (int i = 0; i < col_list.getLength(); i++) {
                    HashMap<String, String> column = new HashMap<String, String>();
                    Node col_node = col_list.item(i);
                    if (col_node.getNodeType() == Node.ELEMENT_NODE) {
                        Element col_element = (Element) col_node;
                        String fk = col_element.getAttribute("fk");
                        String is_pk = col_element.getAttribute("is-pk");
                        String col_name = col_element.getElementsByTagName("name").item(0).getTextContent();
                        String col_type = col_element.getElementsByTagName("type").item(0).getTextContent();
                        column.put("FIELD_NAME", col_name);
                        column.put("FIELD_TYPE", col_type);
                        column.put("PRIMARY_KEY",is_pk);
                        column.put("FOREIGN_KEY", fk);
                        columns.add(column);
                        // System.out.println(fk + " " + col_name + " " + col_type);
                    }
                }
                (this.tables).put("factTable", columns);
            }

            Element fact_table_loc_element = (Element) doc.getElementsByTagName("fact-table").item(0);
            String fact_table_loc = fact_table_loc_element.getElementsByTagName("loc").item(0).getTextContent();
            (this.table_data_loc).put("fact_table", fact_table_loc);
            // System.out.println(fact_table_loc);

            Element aggs_element = (Element) doc.getElementsByTagName("aggregates").item(0);
            NodeList agg_list = aggs_element.getElementsByTagName("agg");
            for (int i = 0; i < agg_list.getLength(); i++) {
                String agg = agg_list.item(i).getTextContent();
                // STORE AGREGATE FUNCTION HERE LATER
                // System.out.println("Aggregate function : "+ agg);
            }
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
    }

    public String getXMLFileName() {
        return this.XMLFILENAME;
    }

    public HashMap<String, ArrayList<HashMap<String, String>>> getTablesSchema() {
        return this.tables;
    }

    public HashMap<String, String> getDataFileNames() {
        return this.table_data_loc;
    }

    public void createTable(HashMap<String, ArrayList<HashMap<String, String>>> tables) {
        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(JDBC_URL, username, password);
            System.out.println("Now creating database...");
            Statement stmt = connection.createStatement();
            String sql;
            sql = "create database if not exists stdwh_db";
            stmt.execute(sql);
            System.out.println("Now using database...");
            sql = "use stdwh_db";
            stmt.execute(sql);
            String prev = "";
            for (HashMap.Entry<String, ArrayList<HashMap<String, String>>> entry : tables.entrySet()) {
                String table_name = entry.getKey();

                ArrayList<HashMap<String, String>> fields = entry.getValue();
                String primary_key = "";
                HashMap<String, String> foreign_key = new HashMap<String, String>();
                sql = "CREATE TABLE " + table_name + " (";
                for (int i = 0; i < fields.size(); i++) {
                    String field_name = fields.get(i).get("FIELD_NAME");
                    sql = sql + " " + field_name;
                    String type = fields.get(i).get("FIELD_TYPE");
                    if (type.equals("string")) {
                        type = "VARCHAR(50)";
                    }
                    sql = sql + " " + type + ",";
                    // System.out.println(fields.get(i).get("PRIMARY_KEY"));
                    if (fields.get(i).get("PRIMARY_KEY").equals("true")) {
                        primary_key += field_name + ",";
                    }
                    if (!(fields.get(i).get("FOREIGN_KEY").equals(""))) {
                        foreign_key.put(field_name, fields.get(i).get("FOREIGN_KEY"));
                    }
                }
                // System.out.println(primary_key);

                if (primary_key != null && primary_key.length() > 0) {
                    primary_key = primary_key.substring(0, primary_key.length() - 1);
                }
                if (!(primary_key.equals(""))) {
                    sql = sql + "PRIMARY KEY ( " + primary_key + " )" + ",";
                }
                for (HashMap.Entry<String, String> entry3 : foreign_key.entrySet()) {
                    String[] words = entry3.getValue().split("\\.");
                    sql = sql + "FOREIGN KEY ";
                    sql = sql + "(" + entry3.getKey() + ") REFERENCES ";
                    sql = sql + "dim_" + words[0] + "(" + words[1] + ")" + ",";
                }
                if ((sql != null) && (sql.length() > 0)) {
                    sql = sql.substring(0, sql.length() - 1);
                }
                sql += ");";
                System.out.println(sql);
                if (table_name.equals("factTable")) {
                    prev = sql;
                } else {
                    stmt.executeUpdate(sql);
                }
            }
            if (!prev.equals("")) {
                stmt.executeUpdate(prev);
            }
        } catch (

        SQLException ex) {
            ex.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        }

    }

    public void loadDimensionTables(HashMap<String, ArrayList<HashMap<String, String>>> tables,
            HashMap<String, String> table_data_loc) {

        try {
            Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(JDBC_URL, username, password);
            Statement stmt = connection.createStatement();
            String sql;
            System.out.println("Now using database...");
            sql = "use stdwh_db";
            stmt.execute(sql);

            for (HashMap.Entry<String, ArrayList<HashMap<String, String>>> entry : tables.entrySet()) {
                String table_name = entry.getKey();
                ArrayList<HashMap<String, String>> fields = entry.getValue();
                if (!table_name.equals("factTable")) {
                    String table_loc = table_data_loc.get(table_name);
                    sql = "INSERT INTO " + table_name + " (";
                    for (int i = 0; i < fields.size(); i++) {
                        String field_name = fields.get(i).get("FIELD_NAME");
                        sql = sql + " " + field_name + ",";
                    }
                    if ((sql != null) && (sql.length() > 0)) {
                        sql = sql.substring(0, sql.length() - 1);
                    }
                    sql = sql + ") VALUES (";
                    try {
                        BufferedReader lineReader = new BufferedReader(new FileReader(table_loc));
                        String lineText = null;
                        // int count = 0;
                        lineReader.readLine(); // skip header line which has column names
                        while ((lineText = lineReader.readLine()) != null) {
                            String sql1 = sql;
                            String[] data = lineText.split(",");
                            for (int i = 0; i < fields.size(); i++) {
                                sql1 = sql1 + '"' + data[i] + '"' + ',';
                            }
                            if ((sql != null) && (sql.length() > 0)) {
                                sql1 = sql1.substring(0, sql1.length() - 1);
                            }
                            sql1 = sql1 + ")";
                            // System.out.println(sql1);
                            stmt.executeUpdate(sql1);
                        }
                        lineReader.close();
                    } catch (IOException ex) {
                        System.err.println(ex);
                    }
                }

            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        } catch (Exception e) {
            // Handle errors for Class.forName
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        ConfigParser parser = new ConfigParser();
        parser.parse("config_v2.xml");
        parser.createTable(parser.getTablesSchema());
        parser.loadDimensionTables(parser.getTablesSchema(), parser.getDataFileNames());
    }
}