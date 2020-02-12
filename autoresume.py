# Copyright (c) 2020 NetApp Inc. - All Rights Reserved
# This sample code is provided AS IS, with no support or warranties of any kind, including but not limited to warranties of merchantability or fitness of any kind, expressed or implied.
#
# Utility for StorageGrid NAS Bridge workaround
# Try to resume xcp copies automatically when commands fail with EStale,
# because SG intermittently returns an Estale error for CREATE/MKDIR/SETATTR.
# There's an option for max retries.  If a retry runs more than a minute,
# then the retry counter is reset to 0.

# Updates: 
#   11 February 2020 - created (Peter Schay)
#
# USAGE
# xcp diag -run autoresume.py [regular xcp args]
# 

# python standard modules
import os
import sys

if 'nfs3' not in sys.builtin_module_names:
		print('Please run with "xcp diag -run ./autoresume.py" followed by a normal xcp command line\n'
			'For example:\n'
			'# xcp diag -run ./autoresume.py copy server:/export/dir targetserver:/export/dir'
		)
		sys.exit(1)

# Python modules which might not be in the local system, but are always built into xcp
import subprocess

# xcp modules
import xcp
import nfs3
import sched
import event
import scan, resume
import parseargs as args

# These are the ops which might get ESTALE from the SG NAS bridge
from nfs3 import SETATTR, WRITE, CREATE, MKDIR, SYMLINK, MKNOD, REMOVE, RMDIR, LINK, RENAME, COMMIT

modops = {nfs3.Procs.names[code] for code in (
	SETATTR, WRITE, CREATE, MKDIR, SYMLINK, MKNOD, REMOVE, RMDIR, LINK, RENAME, COMMIT
)}

curTryOption = args.OptionInfo('-retry', 'current retry number', args.Types.Int, arg='#', default=0)
maxTryOption = args.OptionInfo('-maxtries', 'current retry number', args.Types.Int, arg='#', default=3)
myOpts = [curTryOption, maxTryOption]
scan.copyOptions.extend(myOpts)
resume.resumeOptions.extend(myOpts)

def run(argv):
	xcp.xcp(argv, driver=AutoResume(argv), warn=False)

# Async task gets the events published by the XCP engine
# Look for finished command with an EStale error
class AutoResume(sched.SimpleTask):
	def gRun(self, argv):
		self.stream = self.engine.origin.subscribe()
		while 1:
			evt = (yield self.stream)

			if evt.type == event.Types.FinishCommand:
				if isinstance(evt.error, nfs3.EStale):
					tryResume(self.log.log, argv, evt.runner.cmd, evt.error)
				return

# Start an xcp resume command using the same executable path
# to call this python module again so the next resume can also retry.
# Use os.system to kick it off in a separate process, so that we can
# immediately exit and finish logging and release resources from the
# xcp that's currently running.
def tryResume(log, argv, cmd, error):
	reqtype = str(error).split()[1]
	log('Finished command got ESTALE error on request {}'.format(reqtype), out=True)
	if reqtype not in modops:
		log('{} is not in nfs3 target modification ops; not trying resume.'.format(reqtype))
		return
	resumecmd = '{} diag -run {} resume -id {}'.format(
		sys.executable, argv[0], cmd.index.name
	)
	time = 5 # new shell command will sleep this long before starting xcp resume
	curTry = cmd.options.get(curTryOption)
	maxTries = cmd.options.get(maxTryOption)
	if curTry and cmd.task.elapsed() > 60:
		log('Command lasted {}s so it made progress; resetting the retry count to 0'.format(task.elapsed()), out=True)
		curTry = 0

	if curTry >= maxTries:
		log('Failed.  No more retries.', out=True)
		return

	curTry += 1
	resumecmd += ' -retry {curTry} -maxtries {maxTries}'.format(**vars())

	log('Initiating resume {}/{}'.format(curTry, maxTries), out=True)

	os.system(
		'(sleep {time}; echo "AUTORESUME: {resumecmd}"; {resumecmd})&'.format(**vars())
	)
	log('Kicked off next cmd to run in {}s; current command now exiting.'.format(time), out=True)
	# The system() process is running and will start a new xcp after the sleep
	# The current xcp returns at this point and will exit very soon

