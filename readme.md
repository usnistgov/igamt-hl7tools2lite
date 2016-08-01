#Conversion from HL7Tools to IGAMT Lite

A command line program that depends on hl7tools-domain and hl7tools-service codebase to read specified versions of old profile data, 
and converts it to the new profile data.

All output is written to $HOME/profiles/profile-*.json

### We depend on the following projects:
* hl7tools-domain
* hl7tools-service

### Prerequisite:
	One must run the gov.nist.healthcare.hl7tools.service.util.HL72JSONConverter
	Install hl7tools-domain
	Install hl7toos-service
See the readme.md in the hl7tools-service project.
	

###To run:
	Maven update the igamt-hl7toos2lite project to ensure the latest dependencies are being used.
	Run from inside the eclipse IDE   In the launch configuration, pass versions as program arguments.
###Program to run:
![Program to run](img/program2run.png)
###Program arguments:
![Program arguments](img/programargs.png)
