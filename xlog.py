# Copyright (c) 2019 NetApp Inc. - All Rights Reserved
# This sample code is provided AS IS, with no support or warranties of any kind, including but not limited to warranties of merchantability or fitness of any kind, expressed or implied.
#
# Utility to extract info from the xcp log, report on commands/errors/warnings,
# and suggest commands to repair verification differences on copy/sync targets.
# Please note the log file format could change at any time and break this script
#
# Target repair options are experimental and for expert use only;
# consultation with NetApp support is strongly recommended.

# Updates: 
#   7 November 2019 - created (Peter Schay)
#
# USAGE
# The script dynamically adds a new "xlog" command; run it as follows:
# xcp diag -run xlog.py help xlog
# xcp diag -run xlog.py xlog [options]
#
# With no arguments, xlog will read the default xcp log, usually '/opt/NetApp/xFiles/xcp/xcp.log',
# and print a summary of the commands found in the log
#
# Note that timestamps in the logs are in the local time of the system where xcp ran
# so the age numbers shown will be off if running this utility in a different timezone
#
# TODO:
# - Redesign the xcp log format to be more standard, and use better tools to parse the logs instead

import re
import time

import xcp
import repo
import sched
import basics
import command
import datetime
import parseargs as args
from collections import Counter, OrderedDict

def run(argv):
	xcp.xcp(argv)

logFileOption = args.OptionInfo('-f', 'logfile', args.Types.String, arg='fspath', default=repo.getXcpLogPath())
lineNumbersOption = args.OptionInfo('-n', 'print line numbers')
longOption = args.OptionInfo('-l', 'print source, target, error message for each command')

class LogInfo(object):
	def __init__(self, options):
		# These are the options used to run this xlog utility just now
		# The options for commands in the log are in each LogInfo's cmdOptions
		self.options = options
		self.commands = OrderedDict()
		self.counts = Counter()
		self.indexes = OrderedDict()

# Get basic info for each line in the log
# Line number, raw line text, timestamp
class Entry(object):
	def __init__(self, line, i):
		self.i = i
		self.line = line
		fields = line.split(' ')
		self.xcp = False
		self.banner = False
		self.child = None

		if not line:
			return

		# Most of the info we want is in lines that look like this:
		# xcp 2019-11-04 13:48:20 xcp: [...]
		if fields[0] == 'xcp':
			# If it's a worker process, it also has the pid:
			# xcp (1234) 2019-11-04 13:48:20 xcp: [...]
			if re.match('\([0-9]+\)', fields[1]):
				self.child = int(fields[1].lstrip('(').rstrip(')'))
				fields = fields[1:]
			try:
				self.t = time.strptime(' '.join(fields[1:3]), basics.format)
			except Exception as e:
				print("xlog error line {i}: {line}: {e}".format(**vars()))
				return
			fields = fields[3:]
			if fields[0] == 'xcp:' or ' '.join(fields[:2]) == 'xcp ERROR:':
				self.xcp = True
				fields = fields[1:]
		elif fields[0] == 'XCP':
			# XCP 1.3-5f6c0b4; (c) 2019 NetApp, Inc.; Licensed to ...
			self.banner = True
		elif line[:16] == 'RPC channel dump' \
		 or ' stats: {' in line \
		 or fields[0] == 'Build':
			pass
		else:
			print('xlog: warning: parser unimplemented for line {i}: {line}'.format(**vars()))
		self.fields = fields

# Recreate dictionaries from strings in the log that look like these:
#  {-id: 'autoname_copy_2019-08-17_03.56.47.773764', -snap: '172.20.28.50:/NS/.snapshot/xcp_presync_11012019'}
#  {source: 172.20.28.50:/NS, target: 172.20.28.55:/data}
def parseDict(s):
	t = s.lstrip('{').rstrip('}')
	elems = t.split(' ')
	elems = [u.rstrip(':').rstrip(',') for u in elems]
	elems = [u.strip("'") for u in elems]
	return dict(zip(elems[::2], elems[1::2]))

class LoggedCommand(object):
	def __init__(self, entry, logInfo):
		self.index = None
		self.logInfo = logInfo
		self.name = entry.fields[1]
		self.paths = []
		self.idName = ''
		if self.name == 'sync' and entry.fields[2] == 'dry-run':
			self.name += '.dry-run'
			self.cmdOptions = parseDict(' '.join(entry.fields[3:]))
		else:
			self.cmdOptions = parseDict(' '.join(entry.fields[2:]))
		logInfo.counts[self.name] += 1
		self.key = '{}.{}'.format(self.name, logInfo.counts[self.name])
		logInfo.commands[self.key] = self
		self.entries = [entry]
		self.entry = self.entries[0]
		# It is hard to determine if there was a fatal error
		# because the log is so messy; however we try to do it
		# by looking at the last few log entries.  This method
		# definitely needs more validation to be sure it works,
		# and it would be best to just redesign the log so it can
		# be parsed and analyzed by real tools
		self.failure = 0

		# For verify, figure out if it matches the source and target of a copy or sync
		self.match = None

		# How long ago did the command start
		self.age = basics.getAge(
			datetime.datetime.now(),
			datetime.datetime.fromtimestamp(time.mktime(self.entry.t)),
		)
		# We'll calculate duration from the last log entry
		self.duration = ''

	def checkid(self):
		# Find out if the verify exactly matches a copy,
		# or matches the copy and its target
		# or matches a sync -snap and its target,
		# or reverse-matches one of the above.
		if self.name == 'verify':
			for cmd in self.logInfo.commands.values():
				if cmd.name not in ('copy', 'sync') \
				 or cmd.entry.i >= self.entry.i:
					# Don't look at commands that came after this one
					break

				if not cmd.index:
					continue
				source = cmd.index['source']
				target = cmd.index.get('target')
				if '-snap' in cmd.cmdOptions:
					source = cmd.cmdOptions['-snap']
				if source == self.paths[0] and target == self.paths[1]:
					self.match = cmd
					self.reversed = False
				elif source == self.paths[1] and target == self.paths[0]:
					self.match = cmd
					self.reversed = True

	def __str__(self):

		s = "{:{keywidth}}  {:1}  {:>7} ago ({}) {:>7};  {:{idwidth}}".format(
			self.key,
			self.failure and 'E' or '.',
			self.age,
			time.strftime(basics.format, self.entry.t),
			self.duration,
			self.idName,
			keywidth=self.logInfo.keyWidth,
			idwidth=self.logInfo.idWidth,
		)
		if self.name == 'verify':
			s = s.rstrip() + '  '
			#s += str(self.paths)
			if self.match:
				k = self.reversed and self.match.key + ' reversed' or self.match.key
				s += "({})".format(k)
		if '-snap' in self.cmdOptions:
			s += "; -snap {}".format(self.cmdOptions['-snap'])
		if self.logInfo.options.chose(lineNumbersOption):
			s = '{}: '.format(self.entry.i) + s
		return s

class Runxlog(command.Runner):
	def gRun(self, xlogCmd, catalog):
		logInfo = LogInfo(xlogCmd.options)
		if xlogCmd.options.chose(logFileOption):
			logPath = self.options.get(logFileOption)
			print("logPath: {}".format(logPath))
		else:
			logPath = repo.getXcpLogPath()
		print('reading from: {}'.format(logPath))
		print('logging to: {}'.format(self.log.f))

		# Create a simple Entry object for each nonempty line in the log
		with open(logPath) as f:
			entries = [Entry(line.strip(), i) for i, line in enumerate(f.readlines(), 1) if line]

		# Filter out uninteresting lines
		entries = [e for e in entries if e.xcp and len(e.fields)]

		# This is the main loop to parse all the entries and identify commands
		# After this loop, once we have all the commands and all their entries,
		# a few more loops through the commands are done to get some extra info
		cmd = None
		engineInfo = None
		for e in entries:
			if e.fields[0] == 'Command:':
				# Example
				# xcp 2019-11-04 18:52:02 xcp: Command: scan {-id: 'autoname_copy_2019-08-17_03.56.47.773764', -match: 'x.getPath()==1'}
				cmd = LoggedCommand(e, logInfo)
				engineInfo = None

			elif e.fields[0] == 'Paths:':
				# Example
				# xcp 2019-11-04 16:37:11 xcp: Paths: ['172.20.28.50:/NS/.snapshot/xcp_presync_11012019', '172.20.28.55:/data']
				cmd.paths = eval(''.join(e.fields[1:3]))

			elif e.fields[0] == 'Index:':
				# The index has the source and target (or just source for a scan-only index)
				# Example
				# xcp 2019-11-04 18:52:02 xcp: Index: {source: 172.20.28.50:/NS, target: 172.20.28.55:/data}

				# Note that for sync and sync dry-run, there could 
				# be a -snap option which is the latest source.  
				# In that case this index 'source' logged here is the original baseline copy source
				cmd.index = parseDict(' '.join(e.fields[1:]))

				# The log has this 'Index:' line because either -id or -newid was used
				# So get the id name out from the options
				cmd.idName = cmd.cmdOptions.get('-newid') or cmd.cmdOptions['-id']
				logInfo.indexes[cmd.idName] = cmd.index

			elif e.fields[0] == 'main:' and e.fields[3] == 'runid':
				# This is the start of a new command; its 'Command:' entry is coming up
				# Example
				# xcp 2019-11-04 18:52:02 main: pid 12502 runid 890747532820843
				cmd = None
				engineInfo = None

			elif cmd and not engineInfo and ' '.join(e.fields[:2]) == 'Engine info:':
				# When we see 'Engine info:' the first time it is the beginning of the end
				# The log entry before it should either be the final stats line or an error
				# This is obviously a very brittle kludgy way to determine if it failed,
				# and it is my fault for allowing the log to be so inconsistent and messy
				# Anyway this seems to work but certainly needs more validation
				if cmd.entries[-1].fields[0] == 'ERROR:':
					cmd.failure = cmd.entries[-1]
				engineInfo = 1

			if cmd:
				cmd.entries.append(e)

		logInfo.keyWidth = 0
		logInfo.idWidth = 0
		for cmd in logInfo.commands.values():
			logInfo.keyWidth = max(len(cmd.key), logInfo.keyWidth)
			logInfo.idWidth = max(len(cmd.idName), logInfo.idWidth)

		for cmd in logInfo.commands.values():
			cmd.duration = basics.getAge(
				datetime.datetime.fromtimestamp(time.mktime(cmd.entry.t)),
				datetime.datetime.fromtimestamp(time.mktime(cmd.entries[-1].t))
			)
			cmd.checkid()

		for cmd in logInfo.commands.values():
			assert cmd.duration[0] == '+', "internal error: invalid duration calculation: {}".format(cmd.duration[0])
			# the getAge expects negatives so it adds a + which we don't want here
			cmd.duration = cmd.duration.strip('+')

		print
		print('== Indexes ==')
		nameWidth = 0
		for name in logInfo.indexes:
			nameWidth = max(len(name), nameWidth)

		for name, d in logInfo.indexes.items():
			print('{:{nameWidth}}, {}, {}'.format(name, d['source'], d.get('target', ''), nameWidth=nameWidth))

		print
		print('== Commands ==')
		for cmd in logInfo.commands.values():
			print str(cmd)

 		if 0:
			yield


desc = command.Desc(
	"xlog",
	[
		logFileOption,
		lineNumbersOption,
	],
	"Read the log to summarize commands and errors and optionally repair targets",
	npaths=None,
	runner=Runxlog,
)

xcp.commands.append((desc, Runxlog))
