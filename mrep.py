##
# mrep.py
#
# Copyright (c) 2019 NetApp Inc. - All Rights Reserved
# This sample code is provided AS IS, with no support or warranties of any kind, including but not limited to warranties of merchantability or fitness of any kind, expressed or implied.
#
# DESCRIPTION
# Utility to produce multiple metadata reports after a single scan, because scanning a tree with
# hundreds of millions of files could take a very long time and could impact storage performance.
# Each report is based on a filter.  The list of filters is provided in a text file with a filter
# on each line, using the -filters option.
# This utility is meant to be run by xcp's built-in python 2.7 interpreter, e.g. "xcp diag -run mrep.py"
# The mrep command is implemented as a command runner task, MrepScan, which runs in the main process,
# and a BatchTask for the worker processes to count the files and dirs and get stats for the filters.
# Refer to the output of "xcp help info" for filter expression syntax and examples.
#
# WARNING
# Filter expressions will be processed by xcp at runtime using python 'eval'
# For security, don't accept arbitrary filter strings from unprivileged users.
#
# EXAMPLE
# 1) Put two lines with filter names and expressions in the file f.txt:
#   rootfilter: owner == 'root'
#   petefilter: owner == 'pete'
# 2) Run the mrep command in this module to scan the tree
#   # xcp diag -run mrep.py mrep -filters f.txt server:/export/path
# 3) Find the generated report files (default directory is /tmp):
#   petefilter.csv  petefilter.html  petefilter.json  rootfilter.csv  rootfilter.html  rootfilter.json
#
# HISTORY
# August 8, 2019		  Peter Schay		Created

if __name__ == '__main__':
	print 'Multi-report (mrep) command usage:'
	print '# xcp diag -run mrep.py help mrep'
	print '# xcp diag -run mrep.py mrep <options> -filters <file> <path-to-scan>'
	import sys
	sys.exit()

# Python imports
import os
import io
import json

# Modules from the xcp engine
import rd
import xcp
import scan
import repo
import sched
import report
import command
import xfilter
import parseargs as args

filtersOption = args.Data('-filters', 'one filter for each report',
	arg='file with a python eval-ready filter string on each line'
)

saveOption = args.String('-saveto', 'directory for reports',
	arg='local path', default = '/tmp'
)

# Accept all the scan options for the mrep command but hide them in the help
# Performance, depth, and other options may be useful and some options are not, like -l or -v
scanOptions = list(scan.scanOptions)
for opt in scanOptions:
	opt.hidden = True

desc = command.Desc(
	'mrep',
	[filtersOption, saveOption] + scanOptions,
	'Generate multiple reports from a single scan',
	npaths=1
)

# The run function allows this module to be called via xcp diag -run mrep.py
# Note: a quirk of running it this way is that the log file will be 'xcp.x1.log'
def run(argv):
	xcp.xcp(argv)

class MrepScan(command.Runner):
	def gRun(self, cmd, catalog):
		if cmd.options.chose('-id'):
			raise(sched.ShortError('multi-report does not support offline scanning'))

		# The filters option is required
		# It's a file with one line per filter
		if not cmd.options.chose(filtersOption):
			raise(sched.ShortError('missing {} option'.format(filtersOption)))

		# Parse the filters and tack them onto the options,
		# which will be available in our custom BatchTask
		self.treeStatsList = []
		cmd.options.filters = []
		for s in cmd.get(filtersOption).splitlines():
			if not s:
				continue
			name, t = s.split(':', 1)
			try:
				cmd.options.filters.append(xfilter.Filter(t.strip(), self.engine.osCache, name=name))
			except Exception as e:
				raise sched.ShortError('Error in filter <{}>: {}'.format(s, e))
			# Create a TreeStats object here in the main process 
			# to merge the results for this filter from the child batches
			self.treeStatsList.append(rd.TreeStats())

		# Allow the -newid option to create an offline index in the catalog
		index = None
		newtag = cmd.options.get(repo.newtagOption)
		if newtag:
			index = yield (repo.NewIndexTask(catalog, cmd), None)

		hooks = {
			rd.Hooks.DoBatch: BatchTask,
			rd.Hooks.FinishBatchFun: self.finishedBatch,
		}

		# Run the actual scan and wait for it to complete
		scanTree = self.results = yield (scan.ScanTree(cmd.roots[0], index=index, hooks=hooks), None)

		# Now we have the list of TreeStats for each filter and can generate reports
		os.write(2, 'Generating reports for {} filters and saving in {}\n'.format(
			len(cmd.options.filters), cmd.get(saveOption)))

		xcpInfo = repo.newXFInfo(xcp._prog, xcp._version)
		for xf, ts in zip(cmd.options.filters, self.treeStatsList):
			# We have the filter and the stats
			# Use a Tree to get the json and fill in some metadata gaps for the reports
			tree = rd.Tree()
			tree.actions = rd.Actions()
			tree.stats = ts.stats
			tree.treeStats = ts
			# The treestats are just from the files and dirs in the batches;
			# copy any additional scan info that should also be in the reports
			ts.stats[rd.Stats.UnreadableDirs] = scanTree.stats[rd.Stats.UnreadableDirs]
			ts.stats[rd.Stats.UnreadableFiles] = scanTree.stats[rd.Stats.UnreadableFiles]
			treeInfo = tree.getJsonInfo(cmd.options)
			treeInfo['xcp'] = xcpInfo
			treeInfo['source'] = str(cmd.source)
			# Use 'command' in the report to describe the scan command *and* this particular filter
			# so the filter details will show up in the individual reports.  The comma at
			# the end is just to make the line look better in the html report 
			# Note: xf.source is the filter expression, vs cmd.source which is a path
			treeInfo['command'] = '{}, {}, {}, '.format(cmd, xf.name, xf.source)

			# Using json.dumps would be simpler but json.dump handles unicode better
			jsonData = io.BytesIO()
			json.dump(treeInfo, jsonData)

			for ext, data in [
				('html', report.getReport(xcpInfo, treeInfo, filters=True, html=True)),
				('csv', report.getReport(xcpInfo, treeInfo, filters=True, csv=True)),
				('json', jsonData.getvalue()),
			]:
				path = os.path.join(cmd.get(saveOption), xf.name)
				with open('{}.{}'.format(path, ext), 'w') as outf:
					outf.write(data)

			# Uncomment this to print a human-readable report on the console
			#print '== {} =='.format(treeInfo['command'])
			#print report.getReport(xcpInfo, treeInfo, filters=True)

	# For each completed batch, the xcp scan engine will call this function in the main process
	def finishedBatch(self, batch, batchResult, actions):
		for ts, brts in zip(self.treeStatsList, batchResult.treeStatsList):
			ts.update(brts)

# Each BatchTask runs in a worker process which sends the results back to the main scanner
class BatchTask(sched.SimpleTask):
	def gRun(self, batch, batchResult):
		# For this batch of scanned files and dirs, get the stats for each filter
		# Different filters can count or exclude different files and dirs
		batchResult.treeStatsList = []
		for f in self.engine.options.filters:
			ts = rd.TreeStats()
			files = filter(f.check, batch.files)
			dirs = filter(f.check, batch.dirs)
			ts.stats[rd.Stats.NotMatched] = (len(batch.files) + len(batch.dirs)) - (len(files) + len(dirs))
			ts.count(files, batch.tree.when)
			ts.count(dirs, batch.tree.when)
			batchResult.treeStatsList.append(ts)

			# Save the histogram table counts in the treestats object's stats counter
			# This is because the tables have cython fields and at the time cython extension types
			# were not picklable, so the stats are used to retrieve the info back in the main process
			for table in ts.tables:
				table.save(ts.stats)

		# This task does not do any IO so it does not have any yields
		# Add the unreachable yield just to make this function a generator (an xcp coroutine task)
		if 0:
			yield

# Add the new command to xcp so that run(), above, will be able use it
xcp.commands.append((desc, MrepScan))
