package gov.nist.healthcare.tools.hl7.v2.igamt.hl7tools2lite;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.nist.healthcare.hl7tools.service.util.HL72JSONConverter;
import rx.Observable;
import rx.Subscriber;

// Reads the SQL statements from HL72JSONConverter and writes them to a file all pretty like.
// Said SQL is what extracts data from Frank's database for further processing.
// This routine facilitates testing the SQL from the CLI.

public class SQLExporter implements Runnable {

	public final static File OUTPUT_DIR = new File("src/main/resources");
	Logger log = LoggerFactory.getLogger(SQLExporter.class);
	File file = new File(OUTPUT_DIR, "mdb.sql");
	BufferedWriter writer;

	public SQLExporter() {
		try {
			this.writer = new BufferedWriter(new FileWriter(file));
		} catch (IOException e) {
			log.error("", e);
		}
	}

	@Override
	public void run() {
		
		HL72JSONConverter conv = new HL72JSONConverter("2.8.2");
		
		String[] ss = { conv.SQL4_CODE(), conv.SQL4_COMPONENT(), conv.SQL4_DATAELEMENT(), conv.SQL4_ELEMENT(),
				conv.SQL4_EVENT(), conv.SQL4_FIELD(), conv.SQL4_GROUP(), conv.SQL4_INTERACTION(), conv.SQL4_MESSAGE1(),
				conv.SQL4_MESSAGE2(), conv.SQL4_MESSAGETYPE(), conv.SQL4_SEGMENT(), conv.SQL4_TABLE() };

		Observable<String> myObservable = Observable.from(ss);

		Subscriber<String> mySubscriber = new Subscriber<String>() {

			@Override
			public void onNext(String sql) {
				try {
					writer.write(sql);
					writer.newLine();
				} catch (IOException e) {
					log.error("", e);
				}
			};

			@Override
			public void onCompleted() {
				try {
					writer.close();
				} catch (IOException e) {
					log.error("", e);
				}
				log.info("Done!!");
			};

			@Override
			public void onError(Throwable e) {
				log.error("", e);
			};
		};

		myObservable.subscribe(mySubscriber);
	}

	public static void main(String[] args) {
		SQLExporter app = new SQLExporter();
		app.run();
	}

}
