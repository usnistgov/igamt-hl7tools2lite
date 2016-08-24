package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

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
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Segment;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Table;

public class IdInegrityCheck implements Runnable {

	static Logger log = LoggerFactory.getLogger(IdInegrityCheck.class);
	CmdLineParser CLI = new CmdLineParser(this);

	@Option(name = "-from", required = true, usage = "String value of database name")
	String dbFrom;

	@Option(name = "-to", required = true, usage = "String value of database name")
	String dbTo;

	@Option(name = "-r", usage = "If true output the messages report.")
	boolean report;

	@Option(name = "-v", handler = StringArrayOptionHandler.class, required = true, usage = "String values of hl7Versions to process.")
	String[] hl7Versions;

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

	public IdInegrityCheck(String[] args) throws CmdLineException {
		super();
		try {
			CLI.parseArgument(args);
			mongo = new MongoClient("localhost", 27017);
			mongoFrom = new MongoTemplate(new SimpleMongoDbFactory(mongo, dbFrom));
			mongoTo = new MongoTemplate(new SimpleMongoDbFactory(mongo, dbTo));
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			throw e;
		}
	}

	@Override
	public void run() {

		for (String hl7Version : hl7Versions) {
			boolean abort = false;
			IGDocument igdFrom = findIGDocument(mongoFrom, hl7Version);
			if (igdFrom == null) {
				log.error("No IGD found in " + dbFrom + " for " + hl7Version);
				abort = true;
			}
			IGDocument igdTo = findIGDocument(mongoTo, hl7Version);
			if (igdTo == null) {
				log.error("No IGD found in " + dbTo + " for " + hl7Version);
				abort = true;
			}
			if (abort) {
				return;
			}
		}

		for (String hl7Version : hl7Versions) {
			log.info("Start hl7Version=" + hl7Version);

			// We process messages by getting their references from an IGD. This
			// is
			// because it is the IGD that
			// has the hl7Version not the messages.
			IGDocument igdFrom = findIGDocument(mongoFrom, hl7Version);
			IGDocument igdTo = findIGDocument(mongoTo, hl7Version);

			log.info("Messages==>");
			checkMessages(hl7Version, igdFrom, igdTo);
			// SortedSet<Message> msgsFrom = new TreeSet<Message>(msgComp);
			// msgsFrom.addAll(igdFrom.getProfile().getMessages().getChildren());
			// log.info("msgsFrom=" + msgsFrom);
			// SortedSet<Message> msgsTo = new TreeSet<Message>(msgComp);
			// msgsTo.addAll(igdTo.getProfile().getMessages().getChildren());
			// log.info("msgsTo=" + msgsTo);
			//
			// if (msgsFrom.size() != msgsTo.size()) {
			// log.error("Size mismatch: sizeFrom=" + msgsFrom.size() + "
			// sizeTo=" + msgsTo.size());
			// Set<Message> fromOrTo = new TreeSet<Message>(msgComp);
			// fromOrTo.addAll(msgsFrom);
			// fromOrTo.addAll(msgsTo);
			//
			// Set<Message> tmp = new TreeSet<Message>(msgComp);
			// tmp.addAll(fromOrTo);
			// tmp.retainAll(msgsFrom);
			// fromOrTo.removeAll(tmp);
			//
			// List<String> ids = new ArrayList<String>(fromOrTo.size());
			// for (Message msg : fromOrTo) {
			// ids.add(msg.getStructID());
			// }
			// log.info("fromOrTo size delta=" + ids.size());
			// log.info("fromOrTo id delta=" + ids);
			// }
			//
			// Iterator<Message> itrMsgFrom = msgsFrom.iterator();
			// Iterator<Message> itrMsgTo = msgsFrom.iterator();
			//
			// while (itrMsgFrom.hasNext() && itrMsgTo.hasNext()) {
			// Message from = itrMsgFrom.next();
			// Message to = itrMsgTo.next();
			// int[] match = validate(from.getStructID(), to.getStructID(),
			// from.getId(), to.getId());
			// while (match[0] != 0) {
			// if (match[0] < 0) {
			// from = itrMsgFrom.next();
			// } else {
			// to = itrMsgTo.next();
			// }
			// match = validate(from.getName(), to.getName(), from.getId(),
			// to.getId());
			// }
			// if (match[1] != 0) {
			// log.error("ObjectId mismatch: from=" + from.getName() + " to=" +
			// to.getName());
			// }
			// }

			log.info("Segment==>");
			checkSegments(hl7Version);
			// SortedSet<Segment> segsFrom = new TreeSet<Segment>(segComp);
			// segsFrom.addAll(findSegments(mongoFrom, hl7Version));
			// SortedSet<Segment> segsTo = new TreeSet<Segment>(segComp);
			// segsTo.addAll(findSegments(mongoTo, hl7Version));
			//
			// if (segsFrom.size() != segsTo.size()) {
			// log.error("Size mismatch: sizeFrom=" + segsFrom.size() + "
			// sizeTo=" + segsTo.size());
			// Set<Segment> fromOrTo = new TreeSet<Segment>(segComp);
			// fromOrTo.addAll(segsFrom);
			// fromOrTo.addAll(segsTo);
			//
			// Set<Segment> tmp = new TreeSet<Segment>(segComp);
			// tmp.addAll(fromOrTo);
			// tmp.retainAll(segsFrom);
			// fromOrTo.removeAll(tmp);
			//
			// List<String> ids = new ArrayList<String>(fromOrTo.size());
			// for (Segment seg : fromOrTo) {
			// ids.add(seg.getName());
			// }
			// log.info("fromOrTo size delta=" + ids.size());
			// log.info("fromOrTo id delta=" + ids);
			// }
			//
			// Iterator<Segment> itrSegsFrom = segsFrom.iterator();
			// Iterator<Segment> itrSegsTo = segsTo.iterator();
			//
			// while (itrSegsFrom.hasNext() && itrSegsTo.hasNext()) {
			// Segment from = itrSegsFrom.next();
			// Segment to = itrSegsTo.next();
			// int[] match = validate(from.getName(), to.getName(),
			// from.getId(), to.getId());
			// while (match[0] != 0) {
			// if (match[0] < 0) {
			// from = itrSegsFrom.next();
			// } else {
			// to = itrSegsTo.next();
			// }
			// match = validate(from.getName(), to.getName(), from.getId(),
			// to.getId());
			// }
			// if (match[1] != 0) {
			// log.error("ObjectId mismatch: from=" + from.getName() + " to=" +
			// to.getName());
			// }
			// }

			log.info("Datatype==>");
			checkDatatypes(hl7Version);
			// SortedSet<Datatype> dtsFrom = new TreeSet<Datatype>(dtComp);
			// dtsFrom.addAll(findDatatypes(mongoFrom, hl7Version));
			// SortedSet<Datatype> dtsTo = new TreeSet<Datatype>(dtComp);
			// dtsTo.addAll(findDatatypes(mongoTo, hl7Version));
			//
			// if (dtsFrom.size() != dtsTo.size()) {
			// log.error("Size mismatch: sizeFrom=" + dtsFrom.size() + "
			// sizeTo=" + dtsTo.size());
			// Set<Datatype> fromOrTo = new TreeSet<Datatype>(dtComp);
			// fromOrTo.addAll(dtsFrom);
			// fromOrTo.addAll(dtsTo);
			//
			// Set<Datatype> tmp = new TreeSet<Datatype>(dtComp);
			// tmp.addAll(fromOrTo);
			// tmp.retainAll(dtsFrom);
			// fromOrTo.removeAll(tmp);
			// List<String> ids = new ArrayList<String>(fromOrTo.size());
			// for (Datatype dt : fromOrTo) {
			// ids.add(dt.getName());
			// }
			//
			// log.info("fromOrTo size delta=" + ids.size());
			// log.info("fromOrTo id delta=" + ids);
			// }
			//
			// Iterator<Datatype> itrDtsFrom = dtsFrom.iterator();
			// Iterator<Datatype> itrDtTo = dtsTo.iterator();
			//
			// while (itrDtsFrom.hasNext() && itrDtTo.hasNext()) {
			// Datatype from = itrDtsFrom.next();
			// Datatype to = itrDtTo.next();
			// int[] match = validate(from.getName(), to.getName(),
			// from.getId(), to.getId());
			// while (match[0] != 0) {
			// if (match[0] < 0) {
			// from = itrDtsFrom.next();
			// } else {
			// to = itrDtTo.next();
			// }
			// match = validate(from.getName(), to.getName(), from.getId(),
			// to.getId());
			// }
			// if (match[1] != 0) {
			// log.error("ObjectId mismatch: from=" + from.getName() + " to=" +
			// to.getName());
			// }
			// }

			log.info("Table==>");
			checkTables(hl7Version);
			// SortedSet<Table> tabsFrom = new TreeSet<Table>(tabComp);
			// tabsFrom.addAll(findTables(mongoFrom, hl7Version));
			// SortedSet<Table> tabsTo = new TreeSet<Table>(tabComp);
			// tabsTo.addAll(findTables(mongoTo, hl7Version));
			//
			// if (tabsFrom.size() != tabsTo.size()) {
			// log.error("Size mismatch: sizeFrom=" + tabsFrom.size() + "
			// sizeTo=" + tabsTo.size());
			// Set<Table> fromOrTo = new TreeSet<Table>(tabComp);
			// fromOrTo.addAll(tabsFrom);
			// fromOrTo.addAll(tabsTo);
			//
			// Set<Table> tmp = new TreeSet<Table>(tabComp);
			// tmp.addAll(fromOrTo);
			// tmp.retainAll(tabsFrom);
			// fromOrTo.removeAll(tmp);
			//
			// List<String> ids = new ArrayList<String>(fromOrTo.size());
			// for (Table tab : fromOrTo) {
			// ids.add(tab.getBindingIdentifier());
			// }
			//
			// log.info("fromOrTo size delta=" + ids.size());
			// log.info("fromOrTo id delta=" + ids);
			// }
			//
			// Iterator<Table> itrTabsFrom = tabsFrom.iterator();
			// Iterator<Table> itrTabsTo = tabsTo.iterator();
			//
			// while (itrTabsFrom.hasNext() && itrTabsTo.hasNext()) {
			// Table from = itrTabsFrom.next();
			// Table to = itrTabsTo.next();
			// int[] match = validate(from.getBindingIdentifier(),
			// to.getBindingIdentifier(), from.getId(),
			// to.getId());
			// while (match[0] != 0) {
			// if (match[0] < 0) {
			// from = itrTabsFrom.next();
			// } else {
			// to = itrTabsTo.next();
			// }
			// match = validate(from.getBindingIdentifier(),
			// to.getBindingIdentifier(), from.getId(), to.getId());
			// }
			// if (match[1] != 0) {
			// log.error("ObjectId mismatch: from=" +
			// from.getBindingIdentifier() + " to="
			// + to.getBindingIdentifier());
			// }
			// }

			log.info("Done");
		}
	}

	void checkMessages(String hl7Version, IGDocument igdFrom, IGDocument igdTo) {
		SortedSet<Message> msgsFrom = new TreeSet<Message>(msgComp);
		msgsFrom.addAll(igdFrom.getProfile().getMessages().getChildren());
		log.info("msgsFrom=" + msgsFrom);
		SortedSet<Message> msgsTo = new TreeSet<Message>(msgComp);
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

			if (report) {
				messagesReport(msgsTo);
			}
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

	void checkSegments(String hl7Version) {
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
		}
	}

	void checkDatatypes(String hl7Version) {
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
		}
	}

	void checkTables(String hl7Version) {
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

	public void messagesReport(SortedSet<Message> msgs) {
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

	// int[] validate(Message from, Message to) {
	// int[] rval = { msgComp.compare(from, to), 0 };
	// if (rval[0] != 0) {
	// log.error("StructID mismatch from=" + from.getStructID() + " to=" +
	// to.getStructID());
	// } else {
	// rval[1] = from.getId().compareTo(to.getId());
	// }
	// return rval;
	// }

	// int[] validate(Segment from, Segment to) {
	// int[] rval = { segComp.compare(from, to), 0 };
	// if (rval[0] != 0) {
	// log.error("Name mismatch from=" + from.getName() + " to=" +
	// to.getName());
	// } else {
	// rval[1] = from.getId().compareTo(to.getId());
	// }
	// return rval;
	// }

	// int[] validate(Datatype from, Datatype to) {
	// int[] rval = { dtComp.compare(from, to), 0 };
	// if (rval[0] != 0) {
	// log.error("Name mismatch from=" + from.getName() + " to=" +
	// to.getName());
	// } else {
	// rval[1] = from.getId().compareTo(to.getId());
	// }
	// return rval;
	// }

	// int[] validate(Table from, Table to) {
	// int[] rval = { tabComp.compare(from, to), 0 };
	// if (rval[0] != 0) {
	// log.error("BindingId mismatch from=" + from.getBindingIdentifier() + "
	// to=" + to.getBindingIdentifier());
	// } else {
	// rval[1] = from.getId().compareTo(to.getId());
	// }
	// return rval;
	// }

	IGDocument findIGDocument(MongoOperations mongoOps, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Version);
		Query qry = Query.query(where);
		return mongoOps.findOne(qry, IGDocument.class);
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

	public static void main(String[] args) {
		IdInegrityCheck app;
		try {
			app = new IdInegrityCheck(args);
			app.run();
		} catch (CmdLineException e) {
			log.error("", e);
		}
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
