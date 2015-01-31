# refinery/source
This repository should contain only source code that is used for building
artifacts for the Wikimedia Analytics Refinery.

## refinery-core
Core source code for the Wikimeida Analytics Refinery.

## refinery-hive
Hive UDFs and other Hive related code.

## refinery-tools
Handy Tools used for dealing with Wikimedia Analytics data.

# Archiva
WMF uses Archiva as its sole build dependency and deployment artifact repository.
You can read more about this here: https://wikitech.wikimedia.org/wiki/Archiva.

# Dependencies
All third party dependencies should be mirrored in the WMF Archiva instance at
http://archiva.wikimedia.org/repository/mirrored.  You can follow the instructions
at https://wikitech.wikimedia.org/wiki/Archiva#Uploading_Dependency_Artifacts to
upload dependencies to Archiva.  If you don't have login access to Archiva, ask a
WMF Archiva admin to do this for you.

# Releases and Deployment
Follow the instructions at https://wikitech.wikimedia.org/wiki/Archiva#Deploying_to_Archiva
to set up your ```~/.m2/settings.xml``` with proper deployment credentials.  Ask a WMF Archiva
admin if you don't have these but think you should.

## Snapshots
At any given time, the refinery ```${project.version}``` on the master branch should
be X.X.X-SNAPSHOT.  This will allow you to deploy snapshot builds to Archiva to share with other
developers by just running ```mvn deploy```.

## Releases
To upload a new version of refinery/source artifacts to Archiva:

First run ```mvn release:prepare```.  This will:
- Check that there are no uncommitted changes in the sources
- Check that there are no SNAPSHOT dependencies
- Change the version in the POMs from x-SNAPSHOT to a new version (you will be prompted for the versions to use)
- Transform the git information in the POM to include the final destination of the tag
- Run the project tests against the modified POMs to confirm everything is in working order
- Commit the modified POMs and push
- Tag the code in the git with a version name
- Bump the version in the POMs to a new value y-SNAPSHOT (these values will also be prompted for)
- Commit the modified POMs and push

(Taken from http://maven.apache.org/maven-release/maven-release-plugin/examples/prepare-release.html,
see that for more info on the maven release plugin.)

In addition to the above, there will now be release.properties that contains all the information
needed to perform the current release.  If everything looks good, run ```mvn release:perform```
to build and upload your artifacts to Archiva.  Note: This might take a while.
