#Conversion from HL7Tools to IGAMT Lite

A command line program that uses the hl7tools-* codebase to read all versions of old profile data, 
then converts it to the new profile.json based on the lite* codebase.

All output is written to $HOME/profiles/profile-*.json

### We depend on the following projects:
hl7tools-domain
hl7tools-service
https://github.com/usnistgov/igamt.git

### To build:
	$> mvn package

###To run:
	$> java -jar hl7tools2lite-0.0.1-SNAPSHOT-jar-with-dependencies.jar 