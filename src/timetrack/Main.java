package timetrack;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.json.JSONException;
import org.json.JSONObject;

public class Main {
	public static void main(String args[]) throws IOException, JSONException, ClassNotFoundException, SQLException{
		
		/*
		 * args[0] -> data del giorno da analizzare
		 * 
		 * alternativa
		 * 
		 * no args
		 */
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String date = "";
		int days = 0;
		
		if (args.length == 0){
			//recupero la data di ieri
			Calendar c = GregorianCalendar.getInstance();
			
			c.add(Calendar.DAY_OF_MONTH, -1);
			Date temp_date = c.getTime();
			date = formatter.format(temp_date);
			
		} else if (args.length == 1){
			String temp_year, temp_month, temp_day;
			int int_year = 0, int_month = 0, int_day = 0;
			
			//inizio controlli correttezza data
			date = args[0];
			if (date.length() != 10){
				System.out.println("parametri errati --> data troppo lunga --> yyyy-MM-dd");
				System.exit(1);
			}
			
			temp_year = date.substring(0, 4);
			temp_month = date.substring(5, 7);
			temp_day = date.substring(8, 10);
			
			try{
				int_year = Integer.parseInt(temp_year);
				int_month = Integer.parseInt(temp_month);
				int_day = Integer.parseInt(temp_day);
			} catch (Exception e) {
				System.out.println(e);
				System.out.println("parametri errati --> parsing della data non riuscito");
				System.exit(1);
			}
			
			//controllo grossolani sulla correttezza della data
			if (int_month <= 0 || int_month > 12 || int_day <= 0 || int_day > 31){
				System.out.println("parametri errati --> data illegale 1");
				System.exit(1);
			}
			//controlli più fini sulla correttezza della data
			if (int_day > 30 && (int_month == 4 || int_month == 6 || int_month == 9 || int_month == 11)){
				System.out.println("parametri errati --> data illegale 2");
				System.exit(1);
			}
			if (int_day > 29 && int_month==2){
				System.out.println("parametri errati --> data illegale 3");
				System.exit(1);
			}
			if (int_day == 29 && int_month==2 && (int_year % 4) != 0){
				System.out.println("parametri errati --> data illegale 4");
				System.exit(1);
			}
			//fine controlli sulla correttezza della data
			
		} else {
			System.out.println("errore nel numero di parametri. chiusura del programma.");
			System.exit(1);			
		}
		
		//Lettura file di configurazione
		FileReader reader = new FileReader("/home/tomcat/keygraph/config.json");
		//FileReader reader = new FileReader("./config.json");
		String fileContents = "";

		int j;
		while ((j = reader.read()) != -1) {
			char ch = (char) j;
			fileContents = fileContents + ch;
		}
		
		JSONObject jsonObject;
		JSONObject mysql_conn = null;
		try {
			//Parsing del file Json
			jsonObject = new JSONObject(fileContents);
			mysql_conn = new JSONObject(jsonObject.get("somer").toString());
		} catch (JSONException e) {
			new WriteConsole(e, date + "error in parsing config.json");
			System.exit(1);
		}
		
		/* Configuration constants */
		String host, user, pass, connectionUrl, dbname;
		int port;
		host = mysql_conn.getString("host").toString();
		user = mysql_conn.getString("user").toString();
		pass = mysql_conn.getString("pass").toString();
		dbname = mysql_conn.getString("dbname").toString();
		port = mysql_conn.getInt("port");

		reader.close();
		
		String create_view, update_timetrack, drop_view;
		Statement stm;
		int rs_int;
		Connection conn;
		
		connectionUrl = "jdbc:mysql://" + host + ":" + port + "/" + dbname + "?autoReconnect=true&useSSL=false";
		Class.forName("com.mysql.jdbc.Driver");
		conn =  DriverManager.getConnection(connectionUrl, user, pass);
		stm = conn.createStatement();
		
		drop_view = "drop view if exists somer.vista_temp";
		rs_int = stm.executeUpdate(drop_view);
		
		create_view = "create view somer.vista_temp as ("
				+ "select * "
				+ "from somer.post "
				+ "where date(pubDate) = '" + date + "')";
		rs_int = stm.executeUpdate(create_view);
		
		update_timetrack = "insert into clusters.timetrack " +
			"select q1.data, twitter, facebook, sito, corrieredellasera, larepubblica, ilsole24ore, lagazzettadellosport, lastampa, ilmessaggero, ilrestodelcarlino, corrieredellosport, ilgiornale, avvenire, lanazione, tuttosport, libero, italiaoggi, ilgazzettino, ilfattoquotidiano, ilsecoloxix, iltirreno, ilmattino, ilgiorno, ansa " +
			"from ( " +
				"select date(v.pubDate) as data, count(*) as twitter " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss " +
				"where r.channel = 'twitter' " +
				"group by date(v.pubDate)) as q1 left join ( " +
				"select date(v.pubDate) as data, count(*) as facebook " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss " +
				"where r.channel = 'facebook' " +
				"group by date(v.pubDate)) as q2 on q1.data = q2.data left join ( " +
				"select date(v.pubDate) as data, count(*) as sito " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss " +
				"where r.channel = 'sito'  " +
				"group by date(v.pubDate)) as q3 on q1.data = q3.data left join ( " +
				"select date(v.pubDate) as data, count(*) as corrieredellasera " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'corrieredellasera'  " +
				"group by date(v.pubDate)) as q4 on q1.data = q4.data left join ( " +
				"select date(v.pubDate) as data, count(*) as larepubblica " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join author a on somer.a.id_author = r.id_author " +
				"where a.name = 'larepubblica'  " +
				"group by date(v.pubDate)) as q5 on q1.data = q5.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilsole24ore " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilsole24ore'  " +
				"group by date(v.pubDate)) as q6 on q1.data = q6.data left join ( " +
				"select date(v.pubDate) as data, count(*) as lagazzettadellosport " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'lagazzettadellosport'  " +
				"group by date(v.pubDate)) as q7 on q1.data = q7.data left join ( " +
				"select date(v.pubDate) as data, count(*) as lastampa " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'lastampa'  " +
				"group by date(v.pubDate)) as q8 on q1.data = q8.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilmessaggero " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilmessaggero'  " +
				"group by date(v.pubDate)) as q9 on q1.data = q9.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilrestodelcarlino " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilrestodelcarlino'  " +
				"group by date(v.pubDate)) as q10 on q1.data = q10.data left join ( " +
				"select date(v.pubDate) as data, count(*) as corrieredellosport " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'corrieredellosport'  " +
				"group by date(v.pubDate)) as q11 on q1.data = q11.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilgiornale " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilgiornale'  " +
				"group by date(v.pubDate)) as q12 on q1.data = q12.data left join ( " +
				"select date(v.pubDate) as data, count(*) as avvenire " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'avvenire'  " +
				"group by date(v.pubDate)) as q13 on q1.data = q13.data left join ( " +
				"select date(v.pubDate) as data, count(*) as lanazione " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'lanazione'  " +
				"group by date(v.pubDate)) as q14 on q1.data = q14.data left join ( " +
				"select date(v.pubDate) as data, count(*) as tuttosport " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'tuttosport'  " +
				"group by date(v.pubDate)) as q15 on q1.data = q15.data left join ( " +
				"select date(v.pubDate) as data, count(*) as libero " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'libero'  " +
				"group by date(v.pubDate)) as q16 on q1.data = q16.data left join ( " +
				"select date(v.pubDate) as data, count(*) as italiaoggi " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'italiaoggi'  " +
				"group by date(v.pubDate)) as q17 on q1.data = q17.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilgazzettino " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilgazzettino'  " +
				"group by date(v.pubDate)) as q18 on q1.data = q18.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilfattoquotidiano " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilfattoquotidiano'  " +
				"group by date(v.pubDate)) as q19 on q1.data = q19.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilsecoloxix " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilsecoloxix'  " +
				"group by date(v.pubDate)) as q20 on q1.data = q20.data left join ( " +
				"select date(v.pubDate) as data, count(*) as iltirreno " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'iltirreno'  " +
				"group by date(v.pubDate)) as q21 on q1.data = q21.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilmattino " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilmattino'  " +
				"group by date(v.pubDate)) as q22 on q1.data = q22.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ilgiorno " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ilgiorno'  " +
				"group by date(v.pubDate)) as q23 on q1.data = q23.data left join ( " +
				"select date(v.pubDate) as data, count(*) as ansa " +
				"from somer.vista_temp v join somer.rssfeed r on v.id_rss = r.id_rss join somer.author a on a.id_author = r.id_author " +
				"where a.name = 'ansa' " +
				"group by date(v.pubDate)) as q24 on q1.data = q24.data;";
		
		rs_int = stm.executeUpdate(update_timetrack);

		drop_view = "drop view somer.vista_temp";
		rs_int = stm.executeUpdate(drop_view);
		
		stm.close();
		conn.close();
		
		new WriteConsole(date + ": analisi completata\n");	

		
	}
}
