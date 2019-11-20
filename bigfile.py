# Copyright (c) 2019 NetApp Inc. - All Rights Reserved
# This sample code is provided AS IS, with no support or warranties of any kind, including but not limited to warranties of merchantability or fitness of any kind, expressed or implied.
#
# Utility for Neto
# Use multiple processes to work on a single big file

# Updates: 
#   19 November 2019 - created (Peter Schay)
#
# USAGE
# The script dynamically adds a new "bigfile" command; run it as follows:
# xcp diag -run bigfile.py help bigfile
# xcp diag -run bigfile.py bigfile [options]

import os
import xcp
import scan
import sched
import command
import parseargs as args
from basics import formatSize as fmts

maxPendOption = args.OptionInfo('-maxpend', 'readahead limit', args.Types.Int, arg='# of requests', default=8)

Stats = ['blocks',]

# xcp diag -run xlog.py will run us here
# A quirk of running it this way is that our log file will be /opt/NetApp/xFiles/xcp/xcp.x1.log
def run(argv):
	xcp.xcp(argv)

class RunBigfile(command.Runner):
	def gRun(self, cmd, catalog):
		f = cmd.source.root
		nproc = cmd.options.get('parallel')
		bs = cmd.options.get('bs')
		print('f {}'.format(f))
		chunk = f.a.size / self.engine.parallel
		chunk = bs*(chunk/bs)
		b2c = chunk/bs
		print('file size {}, parallel workers {}, chunksize {} bytes = {} = {} x {}'.format(
			fmts(f.a.size), nproc, chunk, fmts(chunk), b2c, fmts(bs)
		))
		print('workers x blocks = {}, readahead {}'.format(nproc * b2c, self.options.get(maxPendOption)))
		remainder = f.a.size - f.a.size/chunk
		if (remainder):
			self.log.warn('not processing remainder {} bytes'.format(remainder), out=True)

		# Get our custom stats on the console updates
		sched.engine.statsTask.addStats(Stats)

		offset = 0
		workers = []
		for _ in xrange(nproc):
			workers.append(Worker(f, offset, bs, b2c))
			offset += bs*b2c

		# The yield makes this task (instance of RunBigFile) wait for all the workers to finish
		yield (workers, None)

		print('Workers complete.  Processed {} blocks'.format(sched.engine.stats['blocks']))

class Worker(sched.Task):
	# Using process=True tells the engine to fork a process to run this task
	# In the parent process this Worker task instance just waits on a queue and never enters the gRun() code below
	# The child process creates a new engine which runs a copy of the Worker, which enters its gRun and does the actual IO
	# Each child engine will open its own NFS TCP connections for f
	def __init__(self, f, offset, bs, n, process=True):
		g = self.gRun(f, offset, bs, n)
		super(Worker, self).__init__(f, offset, bs, n, producer=g, process=process)

	# Read n blocks of size bs starting at offset
	def gRun(self, f, offset, bs, n):
		end = offset + bs*n
		self.log.log('hello from worker {} from offset {}'.format(os.getpid(), offset), out=True)
		# The gate is like a semaphore to throttle reads
		gate = sched.Gate(self.options.get(maxPendOption), 'readahead limit')

		while offset <= end:
			# Engine gates us here when maximum IO's are pending
			yield (gate, None)

			# Create the task; sched's global engine automatically puts it on the runq
			Read1(f, offset, bs, gate)

			offset += bs

			sched.engine.stats['blocks'] += 1

		# Wait for any pending Read1 tasks to finish
		if not gate.close():
			yield

class Read1(sched.SimpleTask):
	def gRun(self, f, offset, bs, gate):
		call = (yield (f.read(offset, bs), None))
		gate.leave()

options = [
	maxPendOption,
]

# Just grab all the copy command options for now
# Might be useful to add more features like verification
options.extend(scan.copyOptions)

desc = command.Desc(
	'bigfile',
	options,
	'Use multiple processes to work on a single file',
	npaths=1,
	runner=RunBigfile,
)

xcp.commands.append((desc, RunBigfile))
