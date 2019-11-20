# Copyright (c) 2019 NetApp Inc. - All Rights Reserved
# This sample code is provided AS IS, with no support or warranties of any kind, including but not limited to warranties of merchantability or fitness of any kind, expressed or implied.
#
# Utility for Neto and Randy
# Use multiple processes to work on a single big file

# Updates: 
#   19 November 2019 - created (Peter Schay)
#
# USAGE
# The script dynamically adds a new "bigfile" command; run it as follows:
# xcp diag -run bigfile.py help bigfile
# xcp diag -run bigfile.py bigfile [options] <source>
# xcp diag -run bigfile.py bigfile copy [options] <source> <target>
# Source has to be a file
# For copy, the target can be either a file or a directory where it will create or replace the target file

import os
import rd
import xcp
import scan
import nfs3
import sched
import serve
import client
import command
import parseargs as args
from basics import formatSize as fmts

maxPendOption = args.OptionInfo('-maxpend', 'readahead limit', args.Types.Int, arg='# of requests', default=8)

Stats = ['blocks', 'writes']

# xcp diag -run bigfile.py will run us here
# A quirk of running it this way is that our log file will be /opt/NetApp/xFiles/xcp/xcp.x1.log
def run(argv):
	xcp.xcp(argv)

class RunBigfile(command.Runner):
	def gRun(self, cmd, catalog):
		f = cmd.source.root
		bs = cmd.options.get('bs')
		nproc = cmd.options.get('parallel')

		print('source: {}'.format(f))
		assert f.a.type == nfs3.REG, 'source type must be regular file'
		assert f.a.size > nproc*bs, 'source file is too small ({})'.format(fmts(f.a.size))

		if cmd.desc == copyDesc:
			print('target: {}'.format(cmd.target.root))
			if cmd.target.root.a.type == nfs3.REG:
				print('using existing target file {}'.format(cmd.target.root))
				f.copy = cmd.target.root
			else:
				print('creating target file {}/{}'.format(cmd.target, f.name))
				yield (rd.CreateCopyTask(f, cmd.target.root, f.name), None)

		chunk = f.a.size / self.engine.parallel
		chunk = bs*(chunk/bs)
		b2c = chunk/bs
		print('file size {}, parallel workers {}, chunksize {} bytes = {} = {} x {}'.format(
			fmts(f.a.size), nproc, chunk, fmts(chunk), b2c, fmts(bs)
		))
		print('workers x blocks = {}, readahead {}'.format(nproc * b2c, self.options.get(maxPendOption)))
		remainder = f.a.size - chunk*(f.a.size/chunk)

		# Get our custom stats to display on on the console
		sched.engine.statsTask.addStats(Stats)

		offset = 0
		workers = []
		for _ in xrange(nproc):
			workers.append(Worker(f, offset, bs, b2c))
			offset += bs*b2c

		if remainder:
			print('Adding an extra worker to process remainder of {} blocks + {} bytes'.format(
				remainder/bs, remainder-bs*(remainder/bs)))
			workers.append(Worker(f, offset, bs, remainder/bs, remainder=remainder-bs*(remainder/bs)))

		# The yield makes this task (instance of RunBigFile) wait for all the workers to finish
		yield (workers, None)

		if cmd.desc == copyDesc:
			# We don't really need to commit with ONTAP; just doing it in case linux is the target
			yield (f.copy.commit(), None)

		print('Workers complete.  Processed {} blocks'.format(sched.engine.stats['blocks']))

class Worker(sched.Task):
	# Using process=True tells the engine to fork a process to run this task
	# In the parent process this Worker task instance just waits on a queue and never enters the gRun() code below
	# The child process creates a new engine which runs a copy of the Worker, which enters its gRun and does the actual IO
	# Each child engine will open its own NFS TCP connections for f
	def __init__(self, f, offset, bs, n, remainder=None, process=True):
		g = self.gRun(f, offset, bs, n, remainder)
		super(Worker, self).__init__(f, offset, bs, n, remainder, producer=g, process=process)

	# Read n blocks of size bs starting at offset
	def gRun(self, f, offset, bs, n, remainder):
		end = offset + bs*n
		self.log.log('started worker {} offset {} n {} remainder {}'.format(os.getpid(), offset, n, remainder))
		# The gate is like a semaphore to throttle reads
		gate = sched.Gate(self.options.get(maxPendOption), 'readahead limit')
		# If we are copying, create a writeGate which each read task will pass to a write task
		if f.copy:
			writeGate = sched.Gate(self.options.get(maxPendOption), 'pending write limit')
		else:
			writeGate = None

		while offset < end:
			# Engine gates us here when maximum IO's are pending
			yield (gate, None)

			# Create the task; sched's global engine automatically puts it on the runq
			Read1(f, offset, bs, gate, writeGate=writeGate)

			offset += bs

			sched.engine.stats['blocks'] += 1

		if remainder:
			yield (gate, None)
			Read1(f, offset, remainder, gate, writeGate=writeGate)

		# Wait for any pending Read1 tasks to finish
		if not gate.close():
			yield

		if writeGate and not writeGate.close():
			yield

class Write1(sched.SimpleTask):
	def gRun(self, f, offset, data, writeGate):
		# All writes are stable with ONTAP; just using UNSTABLE mode in case the target is non-ONTAP
		yield (f.write(offset, data, stable=nfs3.Stable_mode.UNSTABLE), None)
		sched.engine.stats['writes'] += 1
		writeGate.leave()

class Read1(sched.SimpleTask):
	def gRun(self, f, offset, count, gate, writeGate=None):
		call = (yield (f.read(offset, count), None))
		gate.leave()
		if writeGate:
			yield (writeGate, None)
			Write1(f.copy, offset, call.res.data, writeGate)

options = [
	maxPendOption,
	client.bsizeOption,
	sched.parallelOption,
]

try:
	options.append(serve.dataOption)
	options.append(serve.sbsOption)
except:
	# Not all versions of xcp have the mkdata option so just ignore
	pass

desc = command.Desc(
	'bigfile', options, 'Use multiple processes to read a single file', npaths=1, runner=RunBigfile,
)

copyDesc = command.Desc(
	"copy", options, "Copy a giant file", npaths=2, parent=desc, runner=RunBigfile,
)

xcp.commands.append((desc, RunBigfile))
