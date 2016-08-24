package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.bson.types.ObjectId;
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
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;

public class IGAMTLiteUpdater implements Runnable {

	Logger log = LoggerFactory.getLogger(IGAMTLiteUpdater.class);

	MongoClient mongo;
	MongoOperations mongoOpsFrom = null;
	MongoOperations mongoOpsTo = null;

	@Override
	public void run() {
		mongo = new MongoClient("localhost", 27017);
		mongoOpsFrom = new MongoTemplate(new SimpleMongoDbFactory(mongo, "igamtFrom"));
		mongoOpsTo = new MongoTemplate(new SimpleMongoDbFactory(mongo, "igamtTo"));
		updateMessages(mongoOpsFrom, mongoOpsTo, "2.8.2");
		verifyMessages(mongoOpsFrom, mongoOpsTo, "2.8.2");
	}
	
	void updateSegments(MongoOperations mongoOpsFrom, MongoOperations mongoOpsTo, String hl7Version) {
		
	}

	void updateMessages(MongoOperations mongoOpsFrom, MongoOperations mongoOpsTo, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Version);
		Query qry = Query.query(where);
		IGDocument igdFrom = mongoOpsFrom.findOne(qry, IGDocument.class);
		IGDocument igdTo = mongoOpsTo.findOne(qry, IGDocument.class);

		mongoOpsTo.findAndRemove(createQuery(igdTo.getId()), IGDocument.class);
		igdTo.setId(igdFrom.getId());
		igdTo.getProfile().setId(igdFrom.getProfile().getId());
		igdTo.getProfile().getMessages().setId(igdFrom.getProfile().getMessages().getId());
		igdTo.getProfile().getSegmentLibrary().setId(igdFrom.getProfile().getSegmentLibrary().getId());
		igdTo.getProfile().getDatatypeLibrary().setId(igdFrom.getProfile().getDatatypeLibrary().getId());
		igdTo.getProfile().getTableLibrary().setId(igdFrom.getProfile().getTableLibrary().getId());

		SortedSet<Message> msgsFrom = new TreeSet<Message>(new MessageComparator());
		msgsFrom.addAll(igdFrom.getProfile().getMessages().getChildren());
		SortedSet<Message> msgsTo = new TreeSet<Message>(new MessageComparator());
		msgsTo.addAll(igdTo.getProfile().getMessages().getChildren());

		igdTo.getProfile().getMessages().getChildren().clear();

		assert (msgsTo.size() == msgsFrom.size());
		mongoOpsTo.save(igdTo);

		Iterator<Message> itrFrom = msgsFrom.iterator();
		Iterator<Message> itrTo = msgsTo.iterator();
		while (itrFrom.hasNext() && itrTo.hasNext()) {
			Message msgFrom = itrFrom.next();
			Message msgTo = itrTo.next();
			if (msgFrom.getName().equals(msgTo.getName())) {
				 mongoOpsTo.findAndRemove(createQuery(msgTo.getId()),
				 Message.class);
				msgTo.setId(msgFrom.getId());
				mongoOpsTo.save(msgTo);
			} else {
				log.error("msgFrom=" + msgFrom + " msgTo=" + msgTo);
				break;
			}
		}
		igdTo.getProfile().getMessages().getChildren().addAll(msgsTo);
		mongoOpsTo.save(igdTo);
	}

	void verifyMessages(MongoOperations mongoOpsFrom, MongoOperations mongoOpsTo, String hl7Version) {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Version);
		Query qry = Query.query(where);
		IGDocument igdFrom = mongoOpsFrom.findOne(qry, IGDocument.class);
		IGDocument igdTo = mongoOpsTo.findOne(qry, IGDocument.class);
		assert (igdFrom.getId().equals(igdTo.getId()));
		assert (igdFrom.getProfile().getId().equals(igdTo.getProfile().getId()));
		assert (igdFrom.getProfile().getMessages().getId().equals(igdTo.getProfile().getMessages().getId()));

		SortedSet<Message> msgsFrom = new TreeSet<Message>(new MessageComparator());
		msgsFrom.addAll(igdFrom.getProfile().getMessages().getChildren());
		SortedSet<Message> msgsTo = new TreeSet<Message>(new MessageComparator());
		msgsTo.addAll(igdTo.getProfile().getMessages().getChildren());

		Iterator<Message> itrFrom = msgsFrom.iterator();
		Iterator<Message> itrTo = msgsTo.iterator();
		while (itrFrom.hasNext() && itrTo.hasNext()) {
			Message msgFrom = itrFrom.next();
			Message msgTo = itrTo.next();
			if (msgFrom.getName().equals(msgTo.getName())) {
				assert (msgFrom.getId().equals(msgTo.getId()));
			} else {
				log.error("msgFrom=" + msgFrom + " msgTo=" + msgTo);
				break;
			}
		}
	}

	Query createQuery(String id) {
		Criteria where = Criteria.where("_id").is(new ObjectId(id));
		return Query.query(where);
	}

	class MessageComparator implements Comparator<Message> {

		@Override
		public int compare(Message msg1, Message msg2) {
			return msg1.getName().compareTo(msg2.getName());
		}
	}

	public static void main(String[] args) {
		IGAMTLiteUpdater app = new IGAMTLiteUpdater();
		app.run();
	}
}
