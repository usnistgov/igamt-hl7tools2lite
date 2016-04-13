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

import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.IGDocumentPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.ProfilePreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;

public class IGDocumentInserter implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(IGDocumentInserter.class);

	public final static File INPUT_DIR = new File(System.getenv("LOCAL_IN") + "/igDocuments");

	@Override
	public void run() {

		MongoOperations mongoOps;
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
		mongoOps.dropCollection(Table.class);
		mongoOps.dropCollection(TableLibrary.class);
		mongoOps.dropCollection(Datatype.class);
		mongoOps.dropCollection(DatatypeLibrary.class);
		mongoOps.dropCollection(Segment.class);
		mongoOps.dropCollection(SegmentLibrary.class);
		mongoOps.dropCollection(IGDocument.class);
		ObjectMapper mapper = new ObjectMapper();
		try {
			File[] ff = INPUT_DIR.listFiles();
			for (File f : ff) {
				if (!f.isDirectory()) {
					InputStream is = IGDocument.class.getClassLoader()
							.getResourceAsStream("igDocuments/" + f.getName());
					IGDocumentPreLib appPreLib = mapper.readValue(is, IGDocumentPreLib.class);
					IGDocument app = new IGDocument();
					ProfilePreLib ppl = appPreLib.getProfile();
					Profile prof = new Profile();
					prof.setAccountId(ppl.getAccountId());
					prof.setBaseId(ppl.getBaseId());
					prof.setChanges(ppl.getChanges());
					prof.setComment(ppl.getComment());
					prof.setConstraintId(ppl.getConstraintId());
					prof.setMetaData(ppl.getMetaData());
					prof.setScope(ppl.getScope());
					prof.setSectionContents(ppl.getSectionContents());
					prof.setSectionDescription(ppl.getSectionDescription());
					prof.setSectionPosition(ppl.getSectionPosition());
					prof.setSectionTitle(ppl.getSectionTitle());
					prof.setSourceId(ppl.getSourceId());
					prof.setType(ppl.getType());
					prof.setUsageNote(ppl.getUsageNote());
					prof.setMessages(ppl.getMessages());
					app.addProfile(prof);
					log.info("hl7Version=" + appPreLib.getProfile().getMetaData().getHl7Version());
					for (Segment sm : appPreLib.getProfile().getSegments().getChildren()) {
						app.getProfile().getSegmentLibrary().addSegment(sm);
					}
					for (Datatype sm : appPreLib.getProfile().getDatatypes().getChildren()) {
						app.getProfile().getDatatypeLibrary().addDatatype(sm);
					}
					for (Table sm : appPreLib.getProfile().getTables().getChildren()) {
						app.getProfile().getTableLibrary().addTable(sm);
					}
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
