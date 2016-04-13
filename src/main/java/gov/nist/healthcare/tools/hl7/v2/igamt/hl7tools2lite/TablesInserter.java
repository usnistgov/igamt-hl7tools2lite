package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;

public class TablesInserter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(DataypeExporter.class);

	public final static File INPUT_DIR = new File(System.getenv("LOCAL_IN") + "/tableLibraries");

	@Override
	public void run() {

		MongoOperations mongoOps;
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
		ObjectMapper mapper = new ObjectMapper();
		try {
			File[] ff = INPUT_DIR.listFiles();
			for (File f : ff) {
				if (!f.isDirectory()) {
					InputStream is = TableLibrary.class.getClassLoader()
							.getResourceAsStream("tableLibraries/" + f.getName());
					TableLibrary app = mapper.readValue(is, TableLibrary.class);
					log.info("hl7Version=" + app.getMetaData().getHl7Version());
				mongoOps.insert(app, "table-library");
					for (Table tab : app.getTables()) {
						mongoOps.insert(tab, "table");
					}
				}
			}
		} catch (IOException e) {
			log.error("", e);
		}
	}

	public static void main(String[] args) {
		TablesInserter app = new TablesInserter();
		app.run();
	}
}
