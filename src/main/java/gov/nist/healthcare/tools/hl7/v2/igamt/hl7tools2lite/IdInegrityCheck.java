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

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Component;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Datatype;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLibrary;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.DatatypeLink;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Field;
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
public class IdInegrityCheck implements Runnable {

	static Logger log = LoggerFactory.getLogger(IdInegrityCheck.class);
	CmdLineParser CLI = new CmdLineParser(this);

	@Option(name = "-f", aliases = "--from", required = true, usage = "Name of the database we compare from.")
	String dbFrom;

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

	public IdInegrityCheck(String[] args) {
		super();
		try {
			CLI.parseArgument(args);
			if (help) {
				throw new CmdLineException(CLI, "", null);
			}
			mongo = new MongoClient("localhost", 27017);
			mongoFrom = new MongoTemplate(new SimpleMongoDbFactory(mongo, dbFrom));
			mongoTo = new MongoTemplate(new SimpleMongoDbFactory(mongo, dbTo));
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			System.exit(0);
		}
	}

	@Override
	public void run() {

		for (String hl7Version : hl7Versions) {
			boolean abort = false;

			if (checkIGDocument(mongoFrom, hl7Version)) {
				log.error("No IGD found in " + dbFrom + " for " + hl7Version);
				abort = true;
			}
			if (checkIGDocument(mongoTo, hl7Version)) {
				log.error("No IGD found in " + dbTo + " for " + hl7Version);
				abort = true;
			}
			if (abort) {
				return;
			}
		}

		for (String hl7Version : hl7Versions) {
			log.info("Start hl7Version=" + hl7Version);

			log.info("Tables==>");
			checkTables(hl7Version);

			log.info("TableLibrary==>");
			checkTableLibrary(hl7Version);

			log.info("Datatypes==>");
			checkDatatypes(hl7Version);

			log.info("DatatypeLibrary==>");
			checkDatatypeLibrary(hl7Version);

			log.info("Segments==>");
			checkSegments(hl7Version);

			log.info("SegmentLibrary==>");
			checkSegmentLibrary(hl7Version);

			log.info("Messages==>");
			checkMessages(hl7Version);
		}

		if (report) {
			log.info("*******************************");
			log.info("Message structure reporting...");
			log.info("*******************************");
			for (String hl7Version : hl7Versions) {
				log.info("   Message structure for " + hl7Version);
				messagesReport(findMessages(mongoTo, hl7Version));
			}
		}
		log.info("Done");
	}

	public void checkMessages(String hl7Version) {
		// We get our messages from the IGDocument because not all messages can
		// be queried
		// by hl7Version.
		SortedSet<Message> msgsFrom = new TreeSet<Message>(msgComp);
		IGDocument igdFrom = findIGDocument(mongoFrom, hl7Version);
		msgsFrom.addAll(igdFrom.getProfile().getMessages().getChildren());
		log.info("msgsFrom=" + msgsFrom);
		SortedSet<Message> msgsTo = new TreeSet<Message>(msgComp);
		IGDocument igdTo = findIGDocument(mongoTo, hl7Version);
		msgsTo.addAll(igdTo.getProfile().getMessages().getChildren());
		log.info("msgsTo=" + msgsTo);

		if (msgsFrom.size() != msgsTo.size()) {
			log.error("Size mismatch: sizeFrom=" + msgsFrom.size() + " sizeTo=" + msgsTo.size());
			Set<Message> fromOrTo = new TreeSet<Message>(msgComp);
			fromOrTo.addAll(msgsFrom);
			fromOrTo.addAll(msgsTo);

			Set<Message> tmp = new TreeSet<Message>(msgComp);
			tmp.addAll(fromOrTo);
			tmp.retainAll(msgsFrom);
			fromOrTo.removeAll(tmp);

			List<String> ids = new ArrayList<String>(fromOrTo.size());
			for (Message msg : fromOrTo) {
				ids.add(msg.getStructID());
			}
			log.info("fromOrTo size delta=" + ids.size());
			log.info("fromOrTo id delta=" + ids);

		}

		Iterator<Message> itrMsgFrom = msgsFrom.iterator();
		Iterator<Message> itrMsgTo = msgsFrom.iterator();

		while (itrMsgFrom.hasNext() && itrMsgTo.hasNext()) {
			Message from = itrMsgFrom.next();
			Message to = itrMsgTo.next();
			int[] match = validate(from.getStructID(), to.getStructID(), from.getId(), to.getId());
			while (match[0] != 0) {
				if (match[0] < 0) {
					from = itrMsgFrom.next();
				} else {
					to = itrMsgTo.next();
				}
				match = validate(from.getName(), to.getName(), from.getId(), to.getId());
			}
			if (match[1] != 0) {
				log.error("ObjectId mismatch: from=" + from.getName() + " to=" + to.getName());
			}
		}
	}

	public void checkSegments(String hl7Version) {
		SortedSet<Segment> segsFrom = new TreeSet<Segment>(segComp);
		segsFrom.addAll(findSegments(mongoFrom, hl7Version));
		SortedSet<Segment> segsTo = new TreeSet<Segment>(segComp);
		segsTo.addAll(findSegments(mongoTo, hl7Version));

		if (segsFrom.size() != segsTo.size()) {
			log.error("Size mismatch: sizeFrom=" + segsFrom.size() + " sizeTo=" + segsTo.size());
			Set<Segment> fromOrTo = new TreeSet<Segment>(segComp);
			fromOrTo.addAll(segsFrom);
			fromOrTo.addAll(segsTo);

			Set<Segment> tmp = new TreeSet<Segment>(segComp);
			tmp.addAll(fromOrTo);
			tmp.retainAll(segsFrom);
			fromOrTo.removeAll(tmp);

			List<String> ids = new ArrayList<String>(fromOrTo.size());
			for (Segment seg : fromOrTo) {
				ids.add(seg.getName());
			}
			log.info("fromOrTo size delta=" + ids.size());
			log.info("fromOrTo id delta=" + ids);
		}

		Iterator<Segment> itrSegsFrom = segsFrom.iterator();
		Iterator<Segment> itrSegsTo = segsTo.iterator();

		int tablesInFields = 0;
		while (itrSegsFrom.hasNext() && itrSegsTo.hasNext()) {
			Segment from = itrSegsFrom.next();
			Segment to = itrSegsTo.next();
			int[] match = validate(from.getName(), to.getName(), from.getId(), to.getId());
			while (match[0] != 0) {
				if (match[0] < 0) {
					from = itrSegsFrom.next();
				} else {
					to = itrSegsTo.next();
				}
				match = validate(from.getName(), to.getName(), from.getId(), to.getId());
			}
			if (match[1] != 0) {
				log.error("ObjectId mismatch: from=" + from.getName() + " to=" + to.getName());
			}
			tablesInFields += checkFields4Tables(to);
		}
		log.info("HL7Version=" + hl7Version + " Tables in Fields=" + tablesInFields);
	}

	public int checkFields4Tables(Segment to) {
		int tablesInFields = 0;
		for (Field fld : to.getFields()) {
			tablesInFields += fld.getTables().size();
		}
		return tablesInFields;
	}

	public void checkDatatypes(String hl7Version) {
		SortedSet<Datatype> dtsFrom = new TreeSet<Datatype>(dtComp);
		dtsFrom.addAll(findDatatypes(mongoFrom, hl7Version));
		SortedSet<Datatype> dtsTo = new TreeSet<Datatype>(dtComp);
		dtsTo.addAll(findDatatypes(mongoTo, hl7Version));

		if (dtsFrom.size() != dtsTo.size()) {
			log.error("Size mismatch: sizeFrom=" + dtsFrom.size() + " sizeTo=" + dtsTo.size());
			Set<Datatype> fromOrTo = new TreeSet<Datatype>(dtComp);
			fromOrTo.addAll(dtsFrom);
			fromOrTo.addAll(dtsTo);

			Set<Datatype> tmp = new TreeSet<Datatype>(dtComp);
			tmp.addAll(fromOrTo);
			tmp.retainAll(dtsFrom);
			fromOrTo.removeAll(tmp);
			List<String> ids = new ArrayList<String>(fromOrTo.size());
			for (Datatype dt : fromOrTo) {
				ids.add(dt.getName());
			}

			log.info("fromOrTo size delta=" + ids.size());
			log.info("fromOrTo id delta=" + ids);
		}

		Iterator<Datatype> itrDtsFrom = dtsFrom.iterator();
		Iterator<Datatype> itrDtTo = dtsTo.iterator();

		int tablesInComponents = 0;
		while (itrDtsFrom.hasNext() && itrDtTo.hasNext()) {
			Datatype from = itrDtsFrom.next();
			Datatype to = itrDtTo.next();
			int[] match = validate(from.getName(), to.getName(), from.getId(), to.getId());
			while (match[0] != 0) {
				if (match[0] < 0) {
					from = itrDtsFrom.next();
				} else {
					to = itrDtTo.next();
				}
				match = validate(from.getName(), to.getName(), from.getId(), to.getId());
			}
			if (match[1] != 0) {
				log.error("ObjectId mismatch: from=" + from.getName() + " to=" + to.getName());
			}
			tablesInComponents += checkComponents4Tables(to);
		}
		log.info("HL7Version=" + hl7Version + " Tables in Components=" + tablesInComponents);
	}

	public int checkComponents4Tables(Datatype to) {
		int tablesInComponents = 0;
		for (Component comp : to.getComponents()) {
			tablesInComponents += comp.getTables().size();
		}
		return tablesInComponents;
	}

	public void checkTables(String hl7Version) {
		SortedSet<Table> tabsFrom = new TreeSet<Table>(tabComp);
		tabsFrom.addAll(findTables(mongoFrom, hl7Version));
		SortedSet<Table> tabsTo = new TreeSet<Table>(tabComp);
		tabsTo.addAll(findTables(mongoTo, hl7Version));

		if (tabsFrom.size() != tabsTo.size()) {
			log.error("Size mismatch: sizeFrom=" + tabsFrom.size() + " sizeTo=" + tabsTo.size());
			Set<Table> fromOrTo = new TreeSet<Table>(tabComp);
			fromOrTo.addAll(tabsFrom);
			fromOrTo.addAll(tabsTo);

			Set<Table> tmp = new TreeSet<Table>(tabComp);
			tmp.addAll(fromOrTo);
			tmp.retainAll(tabsFrom);
			fromOrTo.removeAll(tmp);

			List<String> ids = new ArrayList<String>(fromOrTo.size());
			for (Table tab : fromOrTo) {
				ids.add(tab.getBindingIdentifier());
			}

			log.info("fromOrTo size delta=" + ids.size());
			log.info("fromOrTo id delta=" + ids);
		}

		Iterator<Table> itrTabsFrom = tabsFrom.iterator();
		Iterator<Table> itrTabsTo = tabsTo.iterator();

		while (itrTabsFrom.hasNext() && itrTabsTo.hasNext()) {
			Table from = itrTabsFrom.next();
			Table to = itrTabsTo.next();
			int[] match = validate(from.getBindingIdentifier(), to.getBindingIdentifier(), from.getId(), to.getId());
			while (match[0] != 0) {
				if (match[0] < 0) {
					from = itrTabsFrom.next();
				} else {
					to = itrTabsTo.next();
				}
				match = validate(from.getBindingIdentifier(), to.getBindingIdentifier(), from.getId(), to.getId());
			}
			if (match[1] != 0) {
				log.error(
						"ObjectId mismatch: from=" + from.getBindingIdentifier() + " to=" + to.getBindingIdentifier());
			}
		}
	}

	public void checkSegmentLibrary(String hl7Version) {
		SegmentLibrary lib = findSegmentLibrary(mongoTo, hl7Version);
		for (SegmentLink link : lib.getChildren()) {
			if (findSegmentById(mongoTo, link.getId()) == null) {
				log.error("Segment link not found " + link);
			}
		}
		List<Segment> segs = findSegments(mongoTo, hl7Version);
		for (Segment seg : segs) {
			SegmentLink link = lib.findOne(seg.getId());
			if (link == null) {
				log.error("Segment not found in library id=" + seg.getId() + " name=" + seg.getName());
			}
		}
	}

	public void checkDatatypeLibrary(String hl7Version) {
		DatatypeLibrary lib = findDatatypeLibrary(mongoTo, hl7Version);
		for (DatatypeLink link : lib.getChildren()) {
			if (findDatatypeById(mongoTo, link.getId()) == null) {
				log.error("Datatype link not found " + link);
			}
		}
		List<Datatype> dts = findDatatypes(mongoTo, hl7Version);
		for (Datatype dt : dts) {
			DatatypeLink link = lib.findOne(dt.getId());
			if (link == null) {
				log.error("Datatype not found in library id=" + dt.getId() + " name=" + dt.getName());
			}
		}
	}

	public void checkTableLibrary(String hl7Version) {
		TableLibrary lib = findTableLibrary(mongoTo, hl7Version);
		for (TableLink link : lib.getChildren()) {
			if (findTableById(mongoTo, link.getId()) == null) {
				log.error("Table link not found id=" + link.getId() + " bindingIdentifier="
						+ link.getBindingIdentifier());
//				List<Table> tabs = findTableByBindingId(hl7Version, mongoTo, link.getBindingIdentifier());
//				if (tabs.size() > 0) {
//					Table tab1 = tabs.get(0);
//					tab1.setId(link.getId());
//					mongoTo.save(tab1);
////					for (Table tab1 : tabs) {
////						TableLink link1 = lib.findOneTableById(tab1.getId());
////						log.error("Possible duplicate=" + link1.getId() + " bindingIdentifier="
////								+ link1.getBindingIdentifier());
////					}
//				}
			}
		}
		List<Table> tables = findTables(mongoFrom, hl7Version);
		for (Table tab : tables) {
			TableLink link = lib.findOneTableById(tab.getId());
			if (link == null) {
				log.error("Table not found in library id=" + tab.getId() + " bindingIdentifier="
						+ tab.getBindingIdentifier());
			}
		}
	}

	public void messagesReport(List<Message> msgs) {
		int recur = 0;
		for (Message msg : msgs) {
			log.info(messageReport(msg, recur));
		}
	}

	public String messageReport(Message msg, int recur) {
		StringBuilder bld = new StringBuilder();
		bld.append(msg.getStructID());
		bld.append(System.lineSeparator());
		for (SegmentRefOrGroup sog : msg.getChildren()) {
			recur++;
			String s = doSegRefOrGroup(sog, recur);
			bld.append(s);
			recur--;
		}
		return bld.toString();
	}

	String doSegRefOrGroup(SegmentRefOrGroup sog, int recur) {
		StringBuilder bld = new StringBuilder();
		if (Constant.GROUP.equals(sog.getType())) {
			Group grp = (Group) sog;
			bld.append(format(grp.getName(), recur));
			bld.append(" " + grp.getUsage());
			bld.append(" " + grp.getMin());
			bld.append("..");
			bld.append(grp.getMax());
			bld.append(System.lineSeparator());
			for (SegmentRefOrGroup sog1 : grp.getChildren()) {
				recur++;
				String s = doSegRefOrGroup(sog1, recur);
				bld.append(s);
				recur--;
			}
		} else {
			SegmentRef ref = (SegmentRef) sog;
			String s = ref.getRef().getName();
			bld.append(format(s, recur));
			bld.append(" " + sog.getType());
			bld.append(System.lineSeparator());
		}
		return bld.toString();
	}

	String format(String s, int recur) {
		return String.format("%" + recur + 1 * 2 + "d", recur) + " " + s;
	}

	int[] validate(String nameFrom, String nameTo, String idFrom, String idTo) {
		int[] rval = { nameFrom.compareTo(nameTo), 0 };
		if (rval[0] != 0) {
			log.error("Name mismatch from=" + nameFrom + " to=" + nameTo);
		} else {
			rval[1] = idFrom.compareTo(idTo);
		}
		return rval;
	}

	boolean checkIGDocument(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Version);
		Query qry = Query.query(where);
		IGDocument igd = findIGDocument(mongoOps, hl7Version);
		return igd == null;
	}

	IGDocument findIGDocument(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Version);
		Query qry = Query.query(where);
		return mongoOps.findOne(qry, IGDocument.class);
	}

	List<Message> findMessages(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		qry.with(new Sort("structID"));
		return mongoOps.find(qry, Message.class);
	}

	List<Segment> findSegments(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		qry.with(new Sort("name"));
		return mongoOps.find(qry, Segment.class);
	}

	List<Datatype> findDatatypes(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		qry.with(new Sort("name"));
		return mongoOps.find(qry, Datatype.class);
	}

	List<Table> findTables(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version").is(hl7Version);
		Query qry = Query.query(where);
		qry.with(new Sort("bindingIdentifier"));
		return mongoOps.find(qry, Table.class);
	}

	SegmentLibrary findSegmentLibrary(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Version);
		Query qry = Query.query(where);
		return mongoOps.findOne(qry, SegmentLibrary.class);
	}

	Segment findSegmentById(MongoOperations mongoOps, String id) {
		Criteria where = Criteria.where("_id").is(new ObjectId(id));
		Query qry = Query.query(where);
		Segment seg = mongoOps.findOne(qry, Segment.class);
		return seg;
	}

	DatatypeLibrary findDatatypeLibrary(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Version);
		Query qry = Query.query(where);
		return mongoOps.findOne(qry, DatatypeLibrary.class);
	}

	Datatype findDatatypeById(MongoOperations mongoOps, String id) {
		Criteria where = Criteria.where("_id").is(new ObjectId(id));
		Query qry = Query.query(where);
		Datatype seg = mongoOps.findOne(qry, Datatype.class);
		return seg;
	}

	TableLibrary findTableLibrary(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Version);
		Query qry = Query.query(where);
		return mongoOps.findOne(qry, TableLibrary.class);
	}

	Table findTableById(MongoOperations mongoOps, String id) {
		Criteria where = Criteria.where("_id").is(new ObjectId(id));
		Query qry = Query.query(where);
		Table tab = mongoOps.findOne(qry, Table.class);
		return tab;
	}

	List<Table> findTableByBindingId(String hl7Version, MongoOperations mongoOps, String id) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("hl7Version").is(hl7Version)
				.and("bindingIdentifier").is(id);
		Query qry = Query.query(where);
		List<Table> tabs = mongoOps.find(qry, Table.class);
		return tabs;
	}

	public static void main(String[] args) {
		new IdInegrityCheck(args).run();
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
