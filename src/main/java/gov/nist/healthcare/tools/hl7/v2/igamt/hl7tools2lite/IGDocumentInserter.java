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

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;

public class IGDocumentInserter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(IGDocumentInserter.class);

	public final static File INPUT_DIR = new File(System.getenv("LOCAL_IN") + "/igDocuments");

	@Override
	public void run() {

		MongoOperations mongoOps;
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
		ObjectMapper mapper = new ObjectMapper();
		try {
			File[] ff = INPUT_DIR.listFiles();
			for (File f : ff) {
				if (!f.isDirectory()) {
					InputStream is = IGDocument.class.getClassLoader()
							.getResourceAsStream("igDocuments/" + f.getName());
					IGDocument app = mapper.readValue(is, IGDocument.class);
					log.info("hl7Version=" + app.getProfile().getMetaData().getHl7Version());
					mongoOps.insert(app, "igdocument");
					mongoOps.insert(app.getProfile().getSegmentLibrary(), "segment-library");
					for (Segment seg : app.getProfile().getSegmentLibrary().getChildren()) {
						mongoOps.insert(seg, "segment");
					}
					mongoOps.insert(app.getProfile().getDatatypeLibrary(), "datatype-library");
					for (Datatype dt : app.getProfile().getDatatypeLibrary().getChildren()) {
						mongoOps.insert(dt, "datatype");
					}
					mongoOps.insert(app.getProfile().getTableLibrary(), "table-library");
					for (Table tab : app.getProfile().getTableLibrary().getChildren()) {
						mongoOps.insert(tab, "table");
					}
				}
			}
		} catch (IOException e) {
			log.error("", e);
		}
	}

	public static void main(String[] args) {
		IGDocumentInserter app = new IGDocumentInserter();
		app.run();
	}
}
