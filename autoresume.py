# Copyright (c) 2020 NetApp Inc. - All Rights Reserved
# This sample code is provided AS IS, with no support or warranties of any kind, including but not limited to warranties of merchantability or fitness of any kind, expressed or implied.
#
# Utility for StorageGrid NAS Bridge workaround
# Try to resume xcp copies automatically when commands fail with EStale,
# because SG intermittently returns an Estale error for CREATE/MKDIR/SETATTR.
# There's an option for max retries.  If a resume runs more than a minute,
# then the resume counter is reset to 0.

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

curResumeOption = args.OptionInfo('-nresume', 'current resume number', args.Types.Int, arg='#', default=0)
maxResumeOption = args.OptionInfo('-maxresumes', 'max number of resumes', args.Types.Int, arg='#', default=3)
myOpts = [curResumeOption, maxResumeOption]
scan.copyOptions.extend(myOpts)
resume.resumeOptions.extend(myOpts)

# The xcp diag -run autoresume.py command will import this module and call run()
def run(argv):
	# Call xcp's main entry point to start the actual copy or resume command
	# Start the driver task which can detect a fatal error and try to resume
	xcp.xcp(argv, driver=AutoResume(argv), warn=False)

# Async task gets the events published by the XCP engine
# Look for finished command with an EStale error
# Update: also can autoresume after a LOOKUP gets ENoent
# (target lookup happens when a mkdir fails)
class AutoResume(sched.SimpleTask):
	def gRun(self, argv):
		self.stream = self.engine.origin.subscribe()
		while 1:
			evt = (yield self.stream)

			if evt.type == event.Types.FinishCommand:
				if isinstance(evt.error, (nfs3.EStale, nfs3.ENoent)):
					tryResume(self.log.log, argv, evt.runner.cmd, evt.error)
				return

# Start an xcp resume command using the same executable path
# to call this python module again so the next resume can also work.
# Use os.system to kick it off in a separate process, so that we can
# immediately exit and finish logging and release resources from the
# xcp that's currently running.
def tryResume(log, argv, cmd, error):
	errwords = str(error).split()
	reqtype = errwords[1]
	log('Finished command got {} error on request {}'.format(error.__class__.__name__, reqtype), out=True)
	if reqtype not in modops and reqtype != 'LOOKUP':
		log('{} is not a target modification op and not a LOOKUP; not trying resume.'.format(reqtype))
		return

	if reqtype == 'LOOKUP':
		# Unfortunately we don't have references in the error object to know where it came from,
		# so we have to parse the string.  Format is "nfs3 LOOKUP 'a' in 'server:/export/d'"
		lookupd = errwords[4].strip("'")
		if not lookupd.startswith(str(cmd.index.target)):
			log("Lookupd '{}' is not on target '{}'; not autoresuming.".format(lookupd, cmd.index.target))
			return
	elif type(error) != nfs3.EStale:
		# it is a modop (CREATE/MKDIR/...) and got ENoent; that's not a known scenario to do an autoresume
		return

	time = 5 # new shell command will sleep this long before starting xcp resume
	curResume = cmd.options.get(curResumeOption)
	maxResumes = cmd.options.get(maxResumeOption)

	if curResume and cmd.task.elapsed() > 60:
		log('Command lasted {}s so it made progress; resetting the resume count to 0'.format(cmd.task.elapsed()), out=True)
		curResume = 0

	if curResume >= maxResumes:
		log('Failed.  No more retries.', out=True)
		return

	resumecmd = '{} diag -run {} resume -id {}'.format(
		sys.executable, argv[0], cmd.index.name
	)

	curResume += 1
	resumecmd += ' -nresume {curResume} -maxresumes {maxResumes}'.format(**vars())

	log('Initiating resume {}/{}'.format(curResume, maxResumes), out=True)
	os.system(
		'(sleep {time}; echo "AUTORESUME: {resumecmd}"; {resumecmd})&'.format(**vars())
	)
	log('Kicked off next cmd to run in {}s; current command now exiting.'.format(time), out=True)
	# The system() process is running and will start a new xcp after the sleep
	# The current xcp returns at this point and will exit very soon
