The guard framework for refinery/source
=======================================

1. Purpose
2. How to run all guards
3. How to run a given guard
4. How to implement a guard



1. Purpose
==========

The guard framework allows to do integration testing of
'refinery/source' code.

Using the guard framework, it is tried to assure that the
'refinery/source' code still matches the (ever-changing) raw data
streams that it is expected to consume.

For example it is getting used to check if the MediaFileUrlParser
still understands all the requests to upload.wikimedia.org. If at some
point the request stream changes, and MediaFileUrlParser can no longer
understand all such requests, the corresponding guard will fail, and
Analytics team members will get alerted.

So guards do not run tests on pre-defined, static data, but can run
tests on recent, live data.

The framework could for example also get used to check if the
ua_parser UDF is still handling the /current/ top 50 browsers
correctly.



2. How to run all guards
========================

From the directory of this README, running

  ./run_all_guards.sh

runs all available guards.

Run

  ./run_all_guards.sh --help

to get to the script's help page



3. How to run a given guard
===========================

Change into the directory of the guard you want to run (like

  cd MediaFileUrlParser

from the directory of this README, for the MediaFileUrlParser
guard). Then issue

  ./run_guard.sh

to run the guard.

Running

  ./run_guard.sh --help

will show the help page for that script.




4. How to implement a guard
===========================

TL;DR: https://gerrit.wikimedia.org/r/#/c/19484 implements a full
guard in <100 lines.


The bare requirements are:

* Create a subdirectory of this README's directory.
* Put a 'run_guard.sh' script in this directory that (when run) does
  all the integration checks you care about.
* Make sure the script understands the '--help' option.


The guard framework supports you by providing a skeleton run_guard.sh
script and also corresponding Java counterparts. To take advantage of
them, look at https://gerrit.wikimedia.org/r/#/c/19484 , which is the
commit of a full blown guard. The general outline is:

* Create a subdirectory of this README's directory.
* Link '../tools/run_guard.sh' into the directory.
* Create a 'guard_settings.inc' script which contains a
  'echo_guard_input' function that echos the desired input to the
  guard to stdout.
  Typically, this will be a zgrep to some of the sampled TSVs. But any
  way to get to recent data is fine.
* Add a Java class (that matches the guard's name) to the
  org.wikimedia.analytics.refinery.tools.guard package in
  refinery-tools.
  (run_guard.sh will run this Java class' main method)
* By extending StdinGuard, all the needed glue and parameter handling
  is taken care off, and it is sufficient to implement a 'check'
  method that consumes a single line of input and throws a
  GuardException upon errors/issues with that line.
* Finally, for StdinGuard based guards, add a static main method to
  the Java class that instantiates the class and calls its 'doMain'
  method.