package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Profile;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;

public class HL7Tools2LiteConverterTest {
	
	static HL7Tools2LiteConverter sut;

	@BeforeClass
	public static void beforeClass() {
		String[] args = {"-d", "igamt1", "-use", "-v", "2.8.2"};
		 try {
			sut = new HL7Tools2LiteConverter(args);
			sut.hl7Version = "2.8.2";
		} catch (CmdLineException e) {
			e.printStackTrace();
		}
	}
			
	//@Test
	public void testAcquireIGDocument() {
		IGDocument igd = sut.getIgd();
		assertNotNull(igd);
		assertNotNull(igd.getProfile());

		sut.existing = false;
		igd = sut.acquireIGDocument();
		assertNotNull(igd);
		assertNull(igd.getProfile());
		sut.existing = true;
	}

	//@Test
	public void testConvert() {
		fail("Not yet implemented");
	}

	//@Test
	public void testAcquireProfile() {
		Profile p = sut.acquireProfile();
		assertNotNull(p);
		assertNotNull(p.getMessages());
		assertTrue(p.getMessages().getChildren().size() > 0);

		sut.existing = false;
		p = sut.acquireProfile();
		assertNotNull(p);
		assertNotNull(p.getMessages());
		assertEquals(0, p.getMessages().getChildren().size());
		sut.existing = true;
	}

	//@Test
	public void testConvertMessage() {
		sut.convertTables(sut.getIg().getCodeTableLibrary());
		sut.convertDatatypes(sut.getIg().getDatatypeLibrary());
		sut.convertSegments(sut.getIg().getSegmentLibrary());
		gov.nist.healthcare.hl7tools.domain.Message i = sut.getIg().getMessageLibrary().get("ADT_A01");
		int seq = 0;
		Message msg1 = sut.convertMessage(i, seq);
		Message msg2 = sut.acquireMessage("ADT_A01");
		assertNotNull(msg1);
		assertNotNull(msg2);
		assertEquals(msg1.getId(), msg2.getId());
	}

	//@Test
	public void testAcquireMessage() {
		Message msg = sut.acquireMessage("ACK");
		assertNotNull(msg);
	}

	//@Test
	public void testAcquireSegmentLibrary() {
		SegmentLibrary lib = sut.acquireSegmentLibrary();
		assertNotNull(lib);
	}

	//@Test
	public void testConvertTables() {
		fail("Not yet implemented");
	}

	//@Test
	public void testAcquireTableLibrary() {
		TableLibrary lib = sut.acquireTableLibrary();
		assertNotNull(lib);
	}

	//@Test
	public void testCreateTableLibraryMetaData() {
		fail("Not yet implemented");
	}

	//@Test
	public void testConvertTable() {
		fail("Not yet implemented");
	}

	//@Test
	public void testAcquireDatatypeLibrary() {
		DatatypeLibrary lib = sut.acquireDatatypeLibrary();
		assertNotNull(lib);
	}

	//@Test
	public void testCreateDatatypeLibraryMetaData() {
		fail("Not yet implemented");
	}

	//@Test
	public void testConvertDatatype() {
		fail("Not yet implemented");
	}
}
