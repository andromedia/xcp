# Copyright (c) 2019 NetApp Inc. - All Rights Reserved
# This sample code is provided AS IS, with no support or warranties of any kind, including but not limited to warranties of merchantability or fitness of any kind, expressed or implied.
#
# Utility to extract info from the xcp log, report on commands/errors/warnings,
# Please note the log file format could change at any time and break this script
#

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
# - Redesign the xcp log format to be more standard, so better tools can parse the logs

import os
import re
import time
import datetime
from collections import Counter, OrderedDict

import xcp
import repo
import sched
import basics
import command
import parseargs as args

# xcp diag -run xlog.py will run us here
def run(argv):
	xcp.xcp(argv)

logFileOption = args.OptionInfo('-f', 'logfile', args.Types.String, arg='fspath', default=repo.getXcpLogPath())
lineNumbersOption = args.OptionInfo('-n', 'print line numbers')
longOption = args.OptionInfo('-l', 'include source, target, and any fatal error message for each command')
showOption = args.OptionInfo('-show', 'get info about a logged command', args.Types.String, arg='command.# (e.g. verify.2)')
osfixOption = args.OptionInfo('-osfix', 'print commands to fix verify errors', args.Types.String, arg='command.# (e.g. verify.2)')

class LogInfo(object):
	def __init__(self, options):
		# These are the options used to run this xlog utility just now
		# The options for commands in the log are in each LogInfo's cmdOptions
		self.options = options
		self.commands = OrderedDict()
		self.counts = Counter()
		self.indexes = OrderedDict()

		self.keyWidth = 1
		self.idWidth = 1
		self.warnWidth = 1
		self.ageWidth = 1
		self.durationWidth = 1

	def getWidths(self):
		# For console format output, get the max width of some of the columns
		# so that multiple commands are lined up
		for cmd in self.commands.values():
			self.keyWidth = max(len(cmd.key), self.keyWidth)
			self.idWidth = max(len(cmd.idName), self.idWidth)
			self.ageWidth = max(len(cmd.age), self.ageWidth)
			self.durationWidth = max(len(cmd.duration), self.durationWidth)
			if cmd.warnings:
				self.warnWidth = max(len(cmd.wcountstr()), self.warnWidth)

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
		self.warning = False
		self.error = False

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
			elif ' WARNING: ' in line:
				self.fields = fields
				self.warning = Warning(self)
				self.xcp = True

		elif fields[0] == 'XCP':
			# XCP 1.3-5f6c0b4; (c) 2019 NetApp, Inc.; Licensed to ...
			self.banner = True
		# These are miscellaneous kinds of lines in the log that we just ignore for now
		# Here are examples from real logs, of what the following 'or ... or ... pass' code is ignoring
		# These are things like extra xcp diagnostic info, python stack tracebacks, nfs retries, etc:
		#   172.20.28.50 tcp 2049 nfs3 c0 (pending 0, sendq 0, slotq 0, closed False, reopenTask None) stats: {'replies received': 6628, 'partials': 204, 'moved': 38784, 'nospace': 0}
		#   Build date: Thu Sep 19 01:25:50 PDT 2019
		#   pending xid 0x58a3d04c nfs3 MKDIR 'TJ3848500001' in 'mm2nv00201:/mm2c02s01p04/TG_DR7/dss_prod1_s2/dss/LB57/data/SICOMPLETED/DH20101021/PF384850' now 1555616052.83 duetime 1555616112.73 (59.9) self.lastCheck 1555616048.49 (-4.3) sched.getTime 1555616052.73 (-0.1)
		#   File "nfs3.pyx", line 289, in nfs3.Client.setattr3 (nfs3.c:5646)
		#   Failed with 'socket connect to 'edimaxfiler tcp 111 pmap2 c0': [Errno -2] Name or service not known'
		# This utility to summarize the log certainly could understand and summarize all those things;
		# but for now it ignores them:
		elif len(fields) > 1 and (line[:16] == 'RPC channel dump' \
		 or ' stats: {' in line \
		 or fields[0] == 'Build' \
		 or ' '.join(fields[:2]) == 'pending xid' \
		 or 'File "' in ' '.join(fields[0:2]) \
		 or fields[1] == 'setattr3()' \
		 or ' '.join(fields[:2]) == '(c) 2019'\
		 or ' '.join(fields[:4]) == 'Register for a license'\
		 or "Failed with 'socket connect" in ' '.join(fields[0:5])):
			pass
		else:
			#print('xlog: parser unimplemented; ignoring unknown line {i}: {line}'.format(**vars()))
			pass
		self.fields = fields
	def __str__(self):
		return ' '.join(self.fields)

class Warning(object):
	def __init__(self, entry):
		self.entry = entry
		self.ignore = False
		self.fspath = None
		self.fspathParent = None
		self.whichAttrs = None
		self.x = None

		if 'your license will expire' in entry.line:
			self.ignore = True

		line = ' '.join(self.entry.fields)

		self.name = self.entry.fields[0] # cmpdir or compare1
		if 'different attrs' in line:
			# Line looks like one of these:
			# cmpdir 'dirname' WARNING: 172.20.28.50:/NS/.snapshot/snapname/subdir/dirname: different attrs (Mtime)
			# compare1 'filename' WARNING: 172.20.28.50:/NS/.snapshot/snapname/subdir/filename: different attrs (Mtime,Size)

			# Category will look like
			# 'cmpdir: (Owner,Group)' or 'compare1: (Mtime)'
			self.x = self.entry.fields[3][:-1] # strip the : from the end
			attrs = self.entry.fields[-1]
			self.whichAttrs = attrs[1:-1].split(',')
			self.category = self.name + ': ' + self.entry.fields[-1]
		elif (self.name == 'rd' and 'LOOKUP' in self.entry.fields and "'_XCP_tmp'" in self.entry.fields) \
		 or 'file not found' in line:
			# Line looks like:
			# compare1 'filename' WARNING: (error) source file not found on target: nfs3 LOOKUP 'filename' in '172.20.28.55:/data/subdir': nfs3 error 2: no such file or directory

			# 'compare1: not found'
			i = self.entry.fields.index('LOOKUP')
			fname = self.entry.fields[i+1][1:-1] # strip quotes
			dname = self.entry.fields[i+3][1:-1]
			dname = dname.rstrip("'")
			self.x = dname + '/' + fname
			self.category = self.name + ': not found'
		else:
			self.x = None
			self.category = self.name

	@property
	def text(self):
		return ' '.join(self.entry.fields)


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
		self.warnings = []
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
		self.finalStatus = None

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
		# Find out if the verify exactly matches the
		# source and target of a copy or a sync,
		# or a sync -snap and its target,
		# or a reverse-verify match for one of the above
		if self.name == 'verify':
			# TODO: this is a quadratic algorithm; fix that
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

	# console format for summary line
	def wcountstr(self):
		return '{}W'.format(len(self.warnings))

	# For now just output a row of basic info
	# TODO: support different formats
	def fmt(self, long=False, html=False, csvfmt=False):
		if html:
			print "html not implemented yet"
			return
		if csvfmt:
			print "html not implemented yet"
			return
		
		if self.warnings:
			nwstr = self.wcountstr()
		else:
			nwstr = '.'

		delim = '  '
		s = "{:{keywidth}}{delim}{:1}{delim}{:>{ageWidth}} ago ({}){delim}{:>{durationWidth}}{delim}"\
			"{:{idwidth}}{delim}{:>{warnWidth}}".format(
			self.key,
			self.failure and 'E' or '.',
			# The age of the command is how long ago it started (not finished)
			# Note: If you're on the west coast and get a log from the east coast,
			# the age could be in the future, e.g. '+2h5m ago'
			self.age,
			time.strftime(basics.format, self.entry.t),
			self.duration,
			self.idName,
			nwstr,
			keywidth=self.logInfo.keyWidth,
			idwidth=self.logInfo.idWidth,
			warnWidth=self.logInfo.warnWidth,
			delim=delim,
			ageWidth=self.logInfo.ageWidth,
			durationWidth=self.logInfo.durationWidth,
		)

		if self.name == 'verify':
			s = s.rstrip() + delim
			if self.match:
				k = self.reversed and (self.match.key + ' reversed') or self.match.key
				s += "(src+target match {})".format(k)

		if '-snap' in self.cmdOptions:
			s += delim + "-snap {}".format(self.cmdOptions['-snap'])
		if self.logInfo.options.chose(lineNumbersOption):
			s = '{}: '.format(self.entry.i) + s
		if long:
			s += delim + str(self.failure or self.finalStatus)

		return s

	def __str__(self):
		return self.fmt()

class Runxlog(command.Runner):
	def gRun(self, xlogCmd, catalog):
		logInfo = LogInfo(xlogCmd.options)
		if xlogCmd.options.chose(logFileOption):
			logPath = self.options.get(logFileOption)
		else:
			logPath = repo.getXcpLogPath()

		print('reading from: {}'.format(logPath))

		# Not logging anything for now, but we could
		# self.log.f is usually going to be /opt/NetApp/xFiles/xcp/xcp.x1.log
		#print('logging to: {}'.format(self.log.f))

		# Create a simple Entry object for each nonempty line in the log

		# If it's bigger than 50MB, let them know
		size = os.path.getsize(logPath)
		if size > (50<<20):
			print("parsing log entries (this could take a while; the log file is {})...".format(basics.formatSize(size)))

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

			elif e.warning:
				if cmd:
					cmd.warnings.append(e.warning)

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
				else:
					cmd.finalStatus = cmd.entries[-1]
				engineInfo = 1

			if cmd:
				cmd.entries.append(e)

		# Get the duration of the command
		# getAge was written to display modification times; that's why it's called "Age";
		# in this case we are using to show human readable command duration like "15h18m"
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

		if xlogCmd.options.chose(showOption):
			cmdKey = xlogCmd.get(showOption) or xlogCmd.get(osfixOption)
			cmd = logInfo.commands.get(cmdKey)
			if not cmd:
				raise sched.ShortError('command {cmdKey} not found in {logPath}'.format(**vars()))

			print(cmd.fmt())
			print(str(cmd.failure or cmd.finalStatus))
			# TODO: print the engine stats here

			print("Found {} warnings".format(len(cmd.warnings)))
			if not cmd.warnings:
				return
			categories = sorted({w.category for w in filter(lambda w: not w.ignore, cmd.warnings)})
			wlists = [filter(lambda w: w.category == wt and not w.ignore, cmd.warnings) for wt in categories]
			wcounts = [len(wl) for wl in wlists]
			print("warning types:")
			for category, n in zip(categories, wcounts):
				print("  {}: {}".format(category, n))
			print("first two of each type...")
			for wl in wlists:
				for w in wl[:2]:
					print w.text

			return

		print('== Indexes ==')
		nameWidth = 0
		for name in logInfo.indexes:
			nameWidth = max(len(name), nameWidth)

		for name, d in logInfo.indexes.items():
			print('{:{nameWidth}}  {}, {}'.format(name, d['source'], d.get('target', ''), nameWidth=nameWidth))

		logInfo.getWidths()
		print
		print('== Commands ==')
		for cmd in logInfo.commands.values():
			print cmd.fmt(long=xlogCmd.options.chose(longOption))

 		if 0:
			yield

desc = command.Desc(
	"xlog",
	[
		logFileOption,
		lineNumbersOption,
		longOption,
		showOption,
	],
	"Read the log to summarize commands and errors and optionally repair targets",
	npaths=None,
	runner=Runxlog,
)

xcp.commands.append((desc, Runxlog))
