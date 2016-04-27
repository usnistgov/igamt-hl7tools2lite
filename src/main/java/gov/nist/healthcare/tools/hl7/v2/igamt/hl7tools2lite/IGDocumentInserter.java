package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.DocumentMetaDataPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.IGDocumentPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.ProfileMetaDataPreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.ProfilePreLib;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DocumentMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.ProfileMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibraryMetaData;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLink;

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
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
					ProfileMetaDataPreLib pmdpl = ppl.getMetaData();
					ProfileMetaData profileMetaData = new ProfileMetaData();
					profileMetaData.setEncodings(pmdpl.getEncodings());
					profileMetaData.setExt(pmdpl.getExt());
					profileMetaData.setHl7Version(pmdpl.getHl7Version());
					profileMetaData.setOrgName("NIST");
					profileMetaData.setSchemaVersion(pmdpl.getSchemaVersion());
					profileMetaData.setSpecificationName(pmdpl.getSpecificationName());
					profileMetaData.setStatus(pmdpl.getStatus());
					profileMetaData.setSubTitle(pmdpl.getSubTitle());
					profileMetaData.setTopics(pmdpl.getTopics());
					profileMetaData.setType(pmdpl.getType());

					prof.setMetaData(profileMetaData);
					prof.setScope(ppl.getScope());
					prof.setSectionContents(ppl.getSectionContents());
					prof.setSectionDescription(ppl.getSectionDescription());
					prof.setSectionPosition(ppl.getSectionPosition());
					prof.setSectionTitle(ppl.getSectionTitle());
					prof.setSourceId(ppl.getSourceId());
					prof.setType(ppl.getType());
					prof.setUsageNote(ppl.getUsageNote());
					app.addProfile(prof);
					DocumentMetaDataPreLib docMetaDataPreLib = appPreLib.getMetaData();
					DocumentMetaData metaData = new DocumentMetaData();
					metaData.setDate(Constant.mdy.format(new Date()));
					metaData.setExt(docMetaDataPreLib.getExt());
					metaData.setHl7Version(appPreLib.getProfile().getMetaData().getHl7Version());
					metaData.setOrgName(docMetaDataPreLib.getOrgName());
					metaData.setSpecificationName(docMetaDataPreLib.getSpecificationName());
					metaData.setStatus(docMetaDataPreLib.getStatus());
					metaData.setSubTitle(docMetaDataPreLib.getSubTitle());
					metaData.setTitle(docMetaDataPreLib.getTitle());
					metaData.setTopics(docMetaDataPreLib.getTopics());
					app.setMetaData(metaData);
					log.info("hl7Version=" + appPreLib.getProfile().getMetaData().getHl7Version());
					Set<Message> msgsPreLib = appPreLib.getProfile().getMessages().getChildren();
					System.out.println("hl7Version=" + app.getProfile().getMetaData().getHl7Version());
					for (Message sm : msgsPreLib) {
						System.out.println("msgs=" + app.getProfile().getMessages().getChildren().size() + " msg name="
								+ sm.getName() + " children=" + sm.getChildren().size());
						app.getProfile().getMessages().addMessage(sm);
					}
					Set<Segment> segs = appPreLib.getProfile().getSegments().getChildren();
					SegmentLibraryMetaData segMetaData = new SegmentLibraryMetaData();
					segMetaData.setDate(Constant.mdy.format(new Date()));
					segMetaData.setHl7Version(ppl.getMetaData().getHl7Version());
					// segMetaData.setName(ppl.getMetaData().getName());
					segMetaData.setOrgName("NIST");
					// segMetaData.setVersion(ppl.getMetaData().getVersion());
					app.getProfile().getSegmentLibrary().setScope(Constant.SCOPE.HL7STANDARD);
					app.getProfile().getSegmentLibrary().setMetaData(segMetaData);
					for (Segment seg : segs) {
						seg.setScope(Constant.SCOPE.HL7STANDARD);
						if (seg.getId() != null) {
							seg.getLibIds().add(app.getProfile().getSegmentLibrary().getId());
							app.getProfile().getSegmentLibrary()
									.addSegment(new SegmentLink(seg.getId(), seg.getName()));
						} else {
							log.error("Null id seg=" + seg.toString());
						}
					}
					Set<Datatype> dts = appPreLib.getProfile().getDatatypes().getChildren();
					DatatypeLibraryMetaData dtMetaData = new DatatypeLibraryMetaData();
					dtMetaData.setDate(Constant.mdy.format(new Date()));
					dtMetaData.setHl7Version(ppl.getMetaData().getHl7Version());
					// dtMetaData.setName(ppl.getMetaData().getName());
					dtMetaData.setOrgName("NIST");
					// dtMetaData.setVersion(ppl.getMetaData().getVersion());
					app.getProfile().getDatatypeLibrary().setScope(Constant.SCOPE.HL7STANDARD);
					app.getProfile().getDatatypeLibrary().setMetaData(dtMetaData);
					for (Datatype dt : dts) {
						if (dt.getId() != null) {
							dt.setScope(Constant.SCOPE.HL7STANDARD);
							dt.setStatus(Datatype.STATUS.PUBLISHED);
							dt.getLibIds().add(app.getProfile().getDatatypeLibrary().getId());
							app.getProfile().getDatatypeLibrary()
									.addDatatype(new DatatypeLink(dt.getId(), dt.getName()));
						} else {
							log.error("Null id dt=" + dt.toString());
						}
					}
					Set<Table> tabs = appPreLib.getProfile().getTables().getChildren();
					TableLibraryMetaData tabMetaData = new TableLibraryMetaData();
					tabMetaData.setDate(Constant.mdy.format(new Date()));
					tabMetaData.setHl7Version(ppl.getMetaData().getHl7Version());
					// tabMetaData.setName(ppl.getMetaData().getName());
					tabMetaData.setOrgName("NIST");
					// tabMetaData.setVersion(ppl.getMetaData().getVersion());
					app.getProfile().getTableLibrary().setScope(Constant.SCOPE.HL7STANDARD);
					app.getProfile().getTableLibrary().setMetaData(tabMetaData);
					for (Table tab : tabs) {
						tab.setScope(Constant.SCOPE.HL7STANDARD);
						if (tab.getId() != null) {
							tab.getLibIds().add(app.getProfile().getTableLibrary().getId());
							app.getProfile().getTableLibrary()
									.addTable(new TableLink(tab.getId(), tab.getBindingIdentifier()));
						} else {
							log.error("Null id tab=" + tab.toString());
						}
					}
					mongoOps.insert(app.getProfile().getMessages().getChildren(), "message");
					mongoOps.insert(app.getProfile().getSegmentLibrary(), "segment-library");
					mongoOps.insert(segs, "segment");
					DatatypeLibrary xxx = app.getProfile().getDatatypeLibrary();
					mongoOps.insert(xxx, "datatype-library");
					mongoOps.insert(dts, "datatype");
					mongoOps.insert(app.getProfile().getTableLibrary(), "table-library");
					mongoOps.insert(tabs, "table");
					mongoOps.insert(app, "igdocument");
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
