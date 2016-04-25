package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import javax.persistence.ConstraintMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.IGDocumentPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.ProfilePreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibraryMetaData;

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
		mongoOps.dropCollection(Message.class);
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
					app.addProfile(prof);
					log.info("hl7Version=" + appPreLib.getProfile().getMetaData().getHl7Version());
					Set<Message> msgsPreLib = appPreLib.getProfile().getMessages().getChildren();
					System.out.println("hl7Version=" + app.getProfile().getMetaData().getHl7Version());
					for (Message sm : msgsPreLib) {
						System.out.println("msgs=" + app.getProfile().getMessages().getChildren().size() + " msg name=" + sm.getName() + " children=" + sm.getChildren().size());
						app.getProfile().getMessages().addMessage(sm);
					}
					Set<Segment> segs = appPreLib.getProfile().getSegments().getChildren();
					SegmentLibraryMetaData segMetaData = new SegmentLibraryMetaData();
					segMetaData.setDate(ppl.getMetaData().getDate());
					segMetaData.setHl7Version(ppl.getMetaData().getHl7Version());
					segMetaData.setName(ppl.getMetaData().getName());
					segMetaData.setOrgName(ppl.getMetaData().getOrgName());
					segMetaData.setSegmentLibId(app.getProfile().getSegmentLibrary().getId());
					segMetaData.setVersion(ppl.getMetaData().getVersion());
					app.getProfile().getSegmentLibrary().setScope(Constant.SCOPE.HL7STANDARD);
					app.getProfile().getSegmentLibrary().setMetaData(segMetaData);
					for (Segment seg : segs) {
						seg.setScope(Constant.SCOPE.HL7STANDARD);
						if (seg.getId() != null) {
							seg.getLibIds().add(app.getProfile().getSegmentLibrary().getId());
							app.getProfile().getSegmentLibrary().addSegment(seg.getId());
						} else {
							log.error("Null id seg=" + seg.toString());
						}
					}
					Set<Datatype> dts = appPreLib.getProfile().getDatatypes().getChildren();
					DatatypeLibraryMetaData dtMetaData = new DatatypeLibraryMetaData();
					dtMetaData.setDate(ppl.getMetaData().getDate());
					dtMetaData.setHl7Version(ppl.getMetaData().getHl7Version());
					dtMetaData.setName(ppl.getMetaData().getName());
					dtMetaData.setOrgName(ppl.getMetaData().getOrgName());
					dtMetaData.setDatatypLibId(app.getProfile().getDatatypeLibrary().getId());
					dtMetaData.setVersion(ppl.getMetaData().getVersion());
					app.getProfile().getDatatypeLibrary().setScope(Constant.SCOPE.HL7STANDARD);
					app.getProfile().getDatatypeLibrary().setMetaData(dtMetaData);
					for (Datatype dt : dts) {
						if (dt.getId() != null) {
							dt.setScope(Constant.SCOPE.HL7STANDARD);
							dt.setStatus(Datatype.STATUS.PUBLISHED);
							dt.getLibIds().add(app.getProfile().getDatatypeLibrary().getId());
							app.getProfile().getDatatypeLibrary().addDatatype(dt.getId());
						} else {
							log.error("Null id dt=" + dt.toString());
						}
					}
					Set<Table> tabs = appPreLib.getProfile().getTables().getChildren();
					TableLibraryMetaData tabMetaData = new TableLibraryMetaData();
					tabMetaData.setDate(ppl.getMetaData().getDate());
					tabMetaData.setHl7Version(ppl.getMetaData().getHl7Version());
					tabMetaData.setName(ppl.getMetaData().getName());
					tabMetaData.setOrgName(ppl.getMetaData().getOrgName());
					tabMetaData.setTableLibId(app.getProfile().getDatatypeLibrary().getId());
					tabMetaData.setVersion(ppl.getMetaData().getVersion());
					app.getProfile().getTableLibrary().setScope(Constant.SCOPE.HL7STANDARD);
					app.getProfile().getTableLibrary().setMetaData(tabMetaData);					
					for (Table tab : tabs) {
						tab.setScope(Constant.SCOPE.HL7STANDARD);
						if (tab.getId() != null) {
							tab.getLibIds().add(app.getProfile().getTableLibrary().getId());
							app.getProfile().getTableLibrary().addTable(tab.getId());
						} else {
							log.error("Null id tab=" + tab.toString());
						}
					}
					mongoOps.insert(app, "igdocument");
					mongoOps.insert(app.getProfile().getMessages().getChildren(), "message");
					mongoOps.insert(app.getProfile().getSegmentLibrary(), "segment-library");
					mongoOps.insert(segs, "segment");
					mongoOps.insert(app.getProfile().getDatatypeLibrary(), "datatype-library");
					mongoOps.insert(dts, "datatype");
					mongoOps.insert(app.getProfile().getTableLibrary(), "table-library");
					mongoOps.insert(tabs, "table");
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
