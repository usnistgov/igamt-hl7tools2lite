package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;

public class DtsCreateTestData implements Runnable {

	private static final Logger log = LoggerFactory.getLogger(DtsCreateTestData.class);

	public final static File OUTPUT_DIR_LOCAL = new File(System.getenv("LOCAL_OUT") + "/datatypeLibraries");
	public final static File OUTPUT_DIR_IGAMT = new File(System.getenv("IGAMT_OUT") + "/datatypeLibraries");

	@Override
	public void run() {

		if (!OUTPUT_DIR_LOCAL .exists()) {
			OUTPUT_DIR_LOCAL .mkdir();
		}

		if (!OUTPUT_DIR_IGAMT .exists()) {
			OUTPUT_DIR_IGAMT .mkdir();
		}
		
		MongoOperations mongoOps;
//		try {
//			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
//			ObjectMapper mapper = new ObjectMapper();
//
//			DatatypeLibraryReadConverter conv = new DatatypeLibraryReadConverter();
//
//			DBCollection coll = mongoOps.getCollection("datatype-library");
//			BasicDBObject qry = new BasicDBObject();
//			List<BasicDBObject> where = new ArrayList<BasicDBObject>();
//			where.add(new BasicDBObject("scope", "HL7STANDARD"));
//			where.add(new BasicDBObject("metaData.hl7Version", "2.5.1"));
//			qry.put("$and", where);
//			DBCursor curr = coll.find(qry);
//
//			DatatypeLibrary lib = null;
//			while (curr.hasNext()) {
//				DBObject obj = curr.next();
//				lib = conv.convert(obj);
//			}
//
//			lib.setId(null);
//			lib.setScope(Constant.SCOPE.MASTER);
//
//			String hl7Version = lib.getMetaData().getHl7Version();
//			int i = 0;
//			Set<DatatypeLink> temp = new HashSet<DatatypeLink>();
//			for (DatatypeLink dt : lib.getChildren()) {
//				if (i < 5) {
//					temp.add(dt);
//				}
//				i++;
//			}
//			lib.setChildren(temp);
//			mongoOps.insert(lib);
//
//			File outfileLocal = new File(OUTPUT_DIR_LOCAL,
//					"dtLib-" + lib.getScope().name() + "-" + hl7Version + ".json");
//			File outfileIGAMT = new File(OUTPUT_DIR_IGAMT,
//					"dtLib-" + lib.getScope().name() + "-" + hl7Version + ".json");
//			Writer jsonLocal = new FileWriter(outfileLocal);
//			mapper.writerWithDefaultPrettyPrinter().writeValue(jsonLocal, lib);
//			Writer jsonIGAMT = new FileWriter(outfileIGAMT);
//			mapper.writerWithDefaultPrettyPrinter().writeValue(jsonIGAMT, lib);
//		} catch (JsonGenerationException e) {
//			log.error("", e);
//		} catch (JsonMappingException e) {
//			log.error("", e);
//		} catch (IOException e) {
//			log.error("", e);
//		}
	}

	public static void main(String[] args) {
		DtsCreateTestData app = new DtsCreateTestData();
		app.run();
	}
}
