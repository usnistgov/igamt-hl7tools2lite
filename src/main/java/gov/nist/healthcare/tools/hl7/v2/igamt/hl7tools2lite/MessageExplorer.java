package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.util.Set;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import com.mongodb.MongoClient;

import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Constant;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Group;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.IGDocument;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.Message;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRef;
import gov.nist.healthcare.tools.hl7.v2.igamt.lite.domain.SegmentRefOrGroup;

public class MessageExplorer implements Runnable {

	static Logger log = LoggerFactory.getLogger(MessageExplorer.class);
	CmdLineParser CLI = new CmdLineParser(this);

	@Option(name = "-d", required = true, usage = "String value of database name")
	String dbName = "igamt";

	@Option(name = "-i", required = true, usage = "StructID of the message to explore.")
	String structID;

	@Option(name = "-v", handler = StringArrayOptionHandler.class, required = true, usage = "String values of hl7Versions to process.")
	String[] hl7Versions;

	MongoClient mongo;
	MongoOperations mongoOps;
	IGDocument igd;

	public MessageExplorer(String[] args) throws CmdLineException {
		super();
		try {
			CLI.parseArgument(args);
			mongo = new MongoClient("localhost", 27017);
			mongoOps = new MongoTemplate(new SimpleMongoDbFactory(mongo, dbName));
		} catch (CmdLineException e) {
			CLI.printUsage(System.out);
			throw e;
		}
		log.info("Main==>");
	}

	@Override
	public void run() {
		Criteria where = Criteria.where("scope").is(Constant.SCOPE.HL7STANDARD).and("metaData.hl7Version")
				.is(hl7Versions[0]);
		Query qry = Query.query(where);
		IGDocument igd =  mongoOps.findOne(qry, IGDocument.class);
		Set<Message> msgs = igd.getProfile().getMessages().getChildren();
		int recur = 0;
		for (Message msg : msgs) {
			if(structID.equals(msg.getStructID())) {
				log.info("message=" + msg.getStructID());
				for(SegmentRefOrGroup sog : msg.getChildren()) {
					print(sog, recur);
				}
			}
		}
	}
	
	void print(SegmentRefOrGroup sog, int recur) {
		if (sog instanceof Group) {
			Group grp = (Group)sog;
			log.info("grp name=" + String.format("%" + recur + 1 * 2 + "d", recur) + grp.getName());
			for(SegmentRefOrGroup sog1 : grp.getChildren()) {
				recur++;
				print(sog1, recur);
				recur--;
			}
		} else {
			log.info("seg name=" + String.format("%" + recur + 1 * 2 + "d", recur) + ((SegmentRef)sog));
		}
	}

	public static void main(String[] args) {
		MessageExplorer app;
		try {
			app = new MessageExplorer(args);
			app.run();
		} catch (CmdLineException e) {
			log.error("", e);
		}
	}

}
