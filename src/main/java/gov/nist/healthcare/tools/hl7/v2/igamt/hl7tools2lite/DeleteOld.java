package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.bson.types.ObjectId;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.TableLink;

//
// This class assesses the integrity of the IGAMT dataset.
// It compares the state of id values in the IGAMT dataset both before and after the HL7Tools2LiteConverter has been run.
// To do this, one must start with two identical copies of the IGAMT dataset, run the HL7Tools2LiteConverter against one, then
// run IdInegrityCheck against the two. In this context, "from" means the unaltered dataset and "to" means the altered or converted 
// dataset.
// This class produces output based on the "no news is good news philosophy" Only errors and progress are reported. 
//
public class DeleteOld implements Runnable {

	static Logger log = LoggerFactory.getLogger(DeleteOld.class);
	CmdLineParser CLI = new CmdLineParser(this);

	@Option(name = "-t", aliases = "--to", required = true, usage = "Name of the database we compare to.")
	String dbTo;

	@Option(name = "-r", aliases = "--report", usage = "If true output the messages report.")
	boolean report;

	@Option(name = "-v", aliases = "--versions", handler = StringArrayOptionHandler.class, required = true, usage = "Blank delimited set of hl7Versions for which to process. e.g. 2.5.1 2.6 2.7")
	String[] hl7Versions;

	@Option(name = "-h", aliases = "--help", usage = "Print usage.")
	boolean help;

	MongoClient mongo;
	MongoOperations mongoFrom;
	MongoOperations mongoTo;
	IGDocument igd;

	// We use these comparators in multiple places so let's make
	// them fields.
	MessageComparator msgComp = new MessageComparator();
	SegmentComparator segComp = new SegmentComparator();
	DatatypeComparator dtComp = new DatatypeComparator();
	TableComparator tabComp = new TableComparator();

	public DeleteOld(String[] args) {
		super();
		try {
			CLI.parseArgument(args);
			if (help) {
				throw new CmdLineException(CLI, "", null);
			}
			mongo = new MongoClient("localhost", 27017);
			mongoTo = new MongoTemplate(new SimpleMongoDbFactory(mongo, dbTo));
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			System.exit(0);
		}
	}

	@Override
	public void run() {

		for (String hl7Version : hl7Versions) {
			findIGDocument(mongoTo, hl7Version);
			findMessages(mongoTo, hl7Version);
			findSegments(mongoTo, hl7Version);
			findDatatypes(mongoTo, hl7Version);
			findTables(mongoTo, hl7Version);
			findUsers(mongoTo, hl7Version);
		}
		
		Class[] clazzes = {Message.class, Segment.class, Datatype.class, Table.class, SegmentLibrary.class, DatatypeLibrary.class, TableLibrary.class};
		for (String hl7Version : hl7Versions) {
			findIGDocument(mongoTo, hl7Version);
			findMessages(mongoTo, hl7Version);
			findSegments(mongoTo, hl7Version);
			findDatatypes(mongoTo, hl7Version);
			findTables(mongoTo, hl7Version);
			findUsers(mongoTo, hl7Version);
			Criteria where = Criteria.where("hl7Version").is(hl7Version);
			Query qry = Query.query(where);
			List<?> list = mongoTo.find(qry, IGDocument.class);
			log.info(IGDocument.class.getName() + "=" + list.size());
			for (Class clazz : clazzes) {
				Criteria where1 = Criteria.where("hl7Version").is(hl7Version);
				Query qry1 = Query.query(where1);
				List<?> list1 = mongoTo.find(qry1, clazz);
				log.info(clazz.getName() + "=" + list1.size());
			}
		}
		log.info("Done");
	}

	void findIGDocument(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("metaData.hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		mongoOps.remove(qry, IGDocument.class);
	}
	
	void findUsers(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.USER);
		Query qry = Query.query(where);
		mongoOps.remove(qry, IGDocument.class);
		mongoOps.remove(qry, Message.class);
		mongoOps.remove(qry, Segment.class);
		mongoOps.remove(qry, Datatype.class);
		mongoOps.remove(qry, Table.class);
		mongoOps.remove(qry, SegmentLibrary.class);
		mongoOps.remove(qry, DatatypeLibrary.class);
		mongoOps.remove(qry, TableLibrary.class);
	}

	void findMessages(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("metaData.hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		mongoOps.remove(qry, Message.class);
	}

	void findSegments(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		qry.with(new Sort("name"));
		mongoOps.remove(qry, Segment.class);
	}

	void findDatatypes(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		qry.with(new Sort("name"));
		mongoOps.remove(qry, Datatype.class);
	}

	void findTables(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		qry.with(new Sort("bindingIdentifier"));
		mongoOps.remove(qry, Table.class);
	}

	void findSegmentLibrary(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		mongoOps.remove(qry, SegmentLibrary.class);
	}

	void findDatatypeLibrary(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		mongoOps.remove(qry, DatatypeLibrary.class);
	}

	void findTableLibrary(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		mongoOps.remove(qry, TableLibrary.class);
	}

	public static void main(String[] args) {
		new DeleteOld(args).run();
	}

	class MessageComparator implements Comparator<Message> {

		@Override
		public int compare(Message thisOne, Message thatOne) {
			return thisOne.getStructID().compareTo(thatOne.getStructID());
		}
	}

	class SegmentComparator implements Comparator<Segment> {

		@Override
		public int compare(Segment thisOne, Segment thatOne) {
			return thisOne.getName().compareTo(thatOne.getName());
		}
	}

	class DatatypeComparator implements Comparator<Datatype> {

		@Override
		public int compare(Datatype thisOne, Datatype thatOne) {
			return thisOne.getName().compareTo(thatOne.getName());
		}
	}

	class TableComparator implements Comparator<Table> {

		@Override
		public int compare(Table thisOne, Table thatOne) {
			return thisOne.getBindingIdentifier().compareTo(thatOne.getBindingIdentifier());
		}
	}
}
