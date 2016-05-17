package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.converter.DatatypeLibraryReadConverter;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;

public class DatatypesInserterTest {

	static MongoOperations mongoOps;
	static DBCollection coll;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		mongoOps = new MongoTemplate(new SimpleMongoDbFactory(new MongoClient(), "igl"));
		coll = mongoOps.getCollection("datatype-library");
	}

	@Test
	public void test() {
		BasicDBObject qry = new BasicDBObject();
		List<BasicDBObject> where = new ArrayList<BasicDBObject>();
		where.add(new BasicDBObject("scope", "HL7STANDARD"));
		where.add(new BasicDBObject("metaData.hl7Version", "2.5.1"));
		qry.put("$and", where);
		DBCursor cur = coll.find(qry);
		DatatypeLibraryReadConverter cnv = new DatatypeLibraryReadConverter(); 
		while(cur.hasNext()) {
			DBObject source = cur.next();
			DatatypeLibrary sut = cnv.convert(source);
			for ( Datatype dt : sut.getChildren()) {
				assertNotNull(dt);
			}
		}
	}

}
