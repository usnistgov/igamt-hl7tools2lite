package gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite.DataypeExporter;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.constraints.ConformanceStatement;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.constraints.Predicate;

public class DatatypeLibraryTest {
	
	private static final Logger log = LoggerFactory.getLogger(DataypeExporter.class);

	static DatatypeLibrary app;
	static Predicate pred;
	static ConformanceStatement cs;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		try {
			InputStream is = DatatypeLibrary.class.getClassLoader().getResourceAsStream("datatypeLibraries/dtLib-2.5.1-HL7STANDARD.json");
			app = mapper.readValue(is, DatatypeLibrary.class);
			pred = new Predicate();
			Datatype dt = app.findOne("569eac0fd4c623dc6550ecae");
			dt.addPredicate(pred);
			 cs = new ConformanceStatement();
			dt.addConformanceStatement(cs);
		} catch (IOException e) {
			log.error("", e);
		}
	}

	@Test
	public void testFindOne() {
		Datatype dt = app.findOne("569eac0fd4c623dc6550ecae");
		assertNotNull(dt);
	}

	@Test
	public void testFindOneByNameAndByLabel() {
		Datatype sut = app.findOneByNameAndByLabel( "FC",  "FC");
		assertNotNull(sut);
		Datatype sut1 = app.findOneByNameAndByLabel( "FC",  "FC1");
		assertNull(sut1);
		Datatype sut2 = app.findOneByNameAndByLabel( "FC1",  "FC");
		assertNull(sut2);
		Datatype sut3 = app.findOneByNameAndByLabel( "FC1",  "FC1");
		assertNull(sut3);
	}

	@Test
	public void testFindOneComponentString() {
		Component sut = app.findOneComponent( "569eac0fd4c623dc6550ed51");
		assertNotNull(sut);
		Component sut1 = app.findOneComponent( "569eac0fd4c623dc6550ed51x");
		assertNull(sut1);
	}

	@Test
	public void testFindOneComponentStringDatatype() {
		Datatype dt = app.findOneDatatypeByLabel("FC");
		assertNotNull(dt);
		Component sut = app.findOneComponent( "569eac0fd4c623dc6550ed51", dt);
		assertNotNull(sut);
	}

	@Test
	public void testFindOneDatatypeByLabel() {
		Datatype sut = app.findOneDatatypeByLabel("FC");
		assertNotNull(sut);
	}

	@Test
	public void testFindOneDatatypeByBase() {
		Datatype sut = app.findOneDatatypeByBase("FC");
		assertNotNull(sut);
	}

	@Test
	public void testFindOnePredicate() {
		Predicate sut = app.findOnePredicate(pred.getId());
		assertNotNull(sut);
	}

	@Test
	public void testFindOneConformanceStatement() {
		ConformanceStatement sut = app.findOneConformanceStatement(cs.getId());
		assertNotNull(sut);
	}

	@Test
	public void testDeletePredicate() {
		Predicate sut = app.findOnePredicate(pred.getId());
		assertNotNull(sut);
		app.deletePredicate(pred.getId());
		Predicate sut1 = app.findOnePredicate(pred.getId());
		assertNull(sut1);
	}

	@Test
	public void testDeleteConformanceStatement() {
		ConformanceStatement sut = app.findOneConformanceStatement(cs.getId());
		assertNotNull(sut);
		app.deleteConformanceStatement(cs.getId());
		ConformanceStatement sut1 = app.findOneConformanceStatement(cs.getId());
		assertNull(sut1);
	}

//	@Test
//	public void testCloneHashMapOfStringDatatypeHashMapOfStringTable() {
//		DatatypeLibrary sut = app.clone(dtRecords, tableRecords)
//	}

//	@Test
	public void testMerge() {
		fail("Not yet implemented");
	}

//	@Test
	public void testSetPositionsOrder() {
		fail("Not yet implemented");
	}

	@Test
	public void testFindTableById() {
		Table sut = app.findTableById("569eac0fd4c623dc6550e1d6");
		assertNotNull(sut);
		Table sut1 = app.findTableById("xxx");
		assertNull(sut1);
	}

	@Test
	public void testFindTableByBindingIdentifier() {
		Table sut = app.findTableByBindingIdentifier("0354");
		assertNotNull(sut);
		Table sut1 = app.findTableByBindingIdentifier("xxx");
		assertNull(sut1);
	}

}
