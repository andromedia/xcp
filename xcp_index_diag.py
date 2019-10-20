# Copyright (c) 2019 NetApp Inc. - All Rights Reserved
# This sample code is provided AS IS, with no support or warranties of any kind, including but not limited to warranties of merchantability or fitness of any kind, expressed or implied.
#
# Utility to diagnose and repair missing ancestry info in a completed xcp sync index
# This is a temporary workaround for syncs which fail with bug 1246734 "missing parent"
# The batch rebuilding is based on the diag rebuild command and the rebuild tasks in idx,
# but this utility does more in addition to read and rebuild.
#
# Updates: 
#   19 October 2019 - created (Peter Schay)
#   20 October 2019 - first version ready to test
#
# It does 3 passes through the index
# 1) First pass detects missing dirs in the ancestry
# 2) Second pass finds their info and ancestry (it should be in some other batch)
# 3) Final pass will rebuild the index (create index.new and then mv it over the original)
#
# Rebuilding an index already worked and is implemented in diag.py and idx.py
# This reimplementation adds the extra logic to find missing dirs
# and locate their ancestry and then add it into the batches that need it
#
# USAGE
# The script dynamically adds a new "indexdiag" command
# The following are the main ways to run it
# - Print usage
# xcp diag -run xcp_index_diag.py help indexdiag
#
# - Run steps 1 and 2 but not 3 (don't rebuild and replace the index)
# xcp diag -run xcp_index_diag.py indexdiag -id src
#
# - Run all 3 steps (last step builds a new index file with the located ancestry info)
# xcp diag -run xcp_index_diag.py indexdiag -id src -rebuild
#
# - Run all 3 steps and replace the index and remove (actually just rename) the renames file
# xcp diag -run xcp_index_diag.py indexdiag -id src -rebuild -replace
#
#
# NOTES
# All three phases rely on the index reader which consolidates small batches into bigger ones using Multibatch
# After a successful rebuild, run the diagnose phase again to make sure the new batches are not missing ancestry
# The point of the renames file is that it contains the latest info which the last sync discovered
# after building the index, so it always takes precedence - dirs discovered to have been moved or renamed
# might have been indexed with the old info.  After this rebuild the renames are no longer needed because the
# index has been fully updated.
#

import xcp
import idx
import diff
import sched
import sync
import command
import repo
import rd
import scan
import client
import nfs3
import rw
import basics
import parseargs as args
from itertools import chain
import cPickle
import zlib

def run(argv):
	xcp.xcp(argv)

rebuildOption = args.OptionInfo('-rebuild', 'create new index')
replaceOption = args.OptionInfo('-replace', 'replace original index after rebuilding it')
tryMissingOption = args.OptionInfo('-trymissing', 'test code by diagnosing dirs as missing even if they are not', hidden=True)

# 3 passes through the index
Diagnosing = 1,
Locating = 2,
Rebuilding = 3,

# These are supposed to display during the index scan phases, for big indexes
Stats = ["missing", "located", "gotAncestry"]

#
# Tasks based on IndexDiagBatch can look into the actual index batches and find problems
# It runs in a child process so needs to return things in a slightly messy manner
# This messy boilerplace stuff is required so that idx.ProcessBatches can call us back
# The messy part is kept here as-is and it's subclassed below for each of three different phases
class IndexDiagBatch(sched.Task):
	def __init__(self, index, batches, resTube=None, process=True):
		self.resTube = resTube
		producer = self.gRun(index, batches)
		super(IndexDiagBatch, self).__init__(index, batches, producer=producer, process=process)

	# This runs in the parent process to get the results from the child
	def cfun(self, result):
		if self.resTube:
			self.resTube.send(result)

class DiagnoseBatch(IndexDiagBatch):
	def gRun(self, index, batches):
		self.name = "diagnose mb {}".format(idx.getName(batches))
		if 0:
			# Even though the condition is false,
			# this yield makes gRun a generator
			yield

		missing = set()
		mb = idx.MultiBatch(index, batches, self.log)

		for x in chain(mb.files, mb.dirs):
			parfh = x.parfh
			while parfh:
				if parfh not in mb.ancestry:
					missing.add(parfh)
					sched.engine.stats["missing"] += 1
					break
				entry = mb.ancestry[parfh]
				parfh = entry[idx.IdxEnt.PARFH]

			# Dir is not missing but the option treats it as missing for testing
			if self.options.chose(tryMissingOption) and x.a.type == nfs3.DIR and x.digest:
				sched.engine.stats["missing"] += 1
				missing.add(x.fh)

		# This is a child process so results have to support pickling
		# (any python objects are fine; however our cython objects such as File3 are not)
		self.results = missing

# If full ancestry of the filehandle is available then return it all in a list
# Returns either the complete ancestry in a dict, or None
def fullAncestry(dfh, ancestry):
	dirs = {}
	while True:
		if dfh not in ancestry:
			# No luck; complete ancestry not found
			return None
		info = ancestry.get(dfh)
		dirs[dfh] = info
		dfh = info[idx.IdxEnt.PARFH]
		if dfh is None:
			# The root entry has parfh None, so we are successful
			return dirs

# Look for dir metadata entries and ancestry for
# each dir that was found in the Diagnose phase
class LocateBatch(IndexDiagBatch):
	def gRun(self, index, batches):
		self.name = "locate mb {}".format(idx.getName(batches))
		if 0:
			yield

		# located is a map of just the dirs we're looking for, if found
		# gotAncestry is the set of those dirs for which we found full ancestry
		# ancestries has the whole trail of dirs up to the root if available
		located = {}
		gotAncestry = set()
		ancestries = {}

		mb = idx.MultiBatch(index, batches, self.log)
		for dfh in (index.missingDirs):
			# If full ancestry is already found, we are done
			if dfh in index.ancestries:
				continue
			# Whether it's got full ancestry or not, take it if we found it
			if dfh in mb.ancestry:
				sched.engine.stats["located"] += 1
				located[dfh] = mb.ancestry[dfh]
			# Now check if the whole trail back to the root is in there
			a = fullAncestry(dfh, mb.ancestry)
			if a:
				sched.engine.stats["gotAncestry"] += 1
				gotAncestry.add(dfh)
				ancestries.update(a)

		self.results = (located, gotAncestry, ancestries)

class RebuildBatch(IndexDiagBatch):
	def gRun(self, index, batches):
		self.name = "rebuild mb {}".format(idx.getName(batches))
		if 0:
			yield
		mb = idx.MultiBatch(index, batches, self.log)

		# All of mb's data structures have been updated with the snap delta, if there is one,
		# and also the info from the renames file.  Any ancestry that we located in other batches
		# has similarly been updated with the same info, so it should be safe to just merge it in.
		mb.ancestry.update(index.ancestries)

		# TODO:
		# If index.ancestries, containing full ancestry of everything that was missing, is very large
		# then it does not have to be added to every batch.  Over time if you rebuild the index a lot,
		# this will be a problem.  A batch should just have the ancestry it needs.  

		# TODO: if there are dirs in index.locatedDirs for which we did not find full ancestry,
		# it seems like there ought to be a way to see if locatedDirs itself has their ancestry.
		# It might, and if it does then they can be merged in also.

		# Now just save a nice new rebuilt batch in the new index
		id = "rebuild [{}]".format(len(mb.headers))
		self.results = idx.reindex(id, mb.ancestry, mb.files, mb.dirs, stamp=False)

# The IndexDiag task just reads the index and runs the processing task for each multibatch
# When rebuilding, it saves the reencoded data for the new index in the ofile
# The ofile can be None for the Diagnosing and Locating phases; they just add
# information to index.missingDirs, index.locatedDirs, index.gotAncestry, index.ancestries
class IndexDiag(sched.SimpleTask):
	def gRun(self, index, ofile, phase):
		self.name = "read index id '{}'".format(index.name)

		# Create a tube to receive results from batch processing tasks as they finsh
		myEnd, otherEnd = sched.Tube("diff").ends

		if phase == Diagnosing:
			batchTaskClass = DiagnoseBatch
		elif phase == Locating:
			batchTaskClass = LocateBatch
		elif phase == Rebuilding:
			batchTaskClass = RebuildBatch

		idx.ProcessBatches(index, batchTaskClass, otherEnd)

		# Send some tokens to start the reader
		# TODO: Design a better way limit the reader; maybe build this feature into the tube in sched
		for i in range(5):
			myEnd.send(1)

		while 1:
			try:
				result = myEnd.receive()
				if result is None:
					result = yield
	
			except idx.EndOfIndex:
				break

			if phase == Rebuilding:
				newbh, newbd = result
				batchData = idx.encodeIndexBatch(newbh, None, newbd)
				yield (rw.AppendTask(ofile, batchData), None)
			elif phase == Diagnosing:
				index.missingDirs.update(result)
			elif phase == Locating:
				(located, gotAncestry, ancestries) = result
				index.locatedDirs.update(located)
				index.gotAncestry.update(gotAncestry)
				index.ancestries.update(ancestries)

			myEnd.send(1)

		if phase == Rebuilding:
			(trailer, tdata) = idx.getTrailer("sync trailer")
			yield (rw.AppendTask(ofile, tdata), None)

class RunIndexDiag(command.Runner):
	def gRun(self, cmd, catalog):
		self.name = "index diag"
		index, jsonInfo = yield (scan.GetCopyInfo(cmd, catalog), None)
		self.name = "diag index id '{}'".format(index.name)
		sched.engine.statsTask.addStats(Stats)

		# missingDirs is a set of filehandles
		# locatedDirs is a mapping of those filehandles to index tuples (i.e. full metadata about the dir)
		# gotAncestry is a set of handles for which we found full ancestry; hopefully all locatedDirs have full ancestry too
		# ancestries is a mapping from located dir handles to lists of their index tuples back to the root
		# The rebuild phase will add the ancestries back into each multibatch so they are not missing anymore
		# We track the locatedDirs and gotAncesty separately so they can be counted and compared to missingDirs;
		# It is possible that some might not have full ancestries found anywhere which would be annoying
		index.missingDirs = set()
		index.locatedDirs = {}
		index.gotAncestry = set()
		index.ancestries = {}

		# Run each pass through the index
		# Diagnosing find dirs that were missing from the ancestry of some batch
		# Locating uses the results of the Diagnosing phase to find full ancestry for those
		# Rebuilding uses the results of the Locating phase, and creates a viable index with complete ancestry

		## Phase 1: Diagnosing ##
		# Read the index and process the batches
		self.log.log("index diagnostics phase 1 (Diagnosing)", out=True)
		yield (IndexDiag(index, None, Diagnosing), None)

		# Index pass is done.  Report what we found and save it
		self.log.log("  total missing {}".format(len(index.missingDirs)), out=True)

		## Phase 2: Locating ##
		self.log.log("\n", out=True)
		self.log.log("index diagnostics phase 2 (Locating)", out=True)
		yield (IndexDiag(index, None, Locating), None)
		self.log.log("  total missing {} located {} gotAncestry {} ancestries {}".format(
			len(index.missingDirs), len(index.locatedDirs), len(index.gotAncestry), len(index.ancestries)), out=True)

		## Phase 3: Rebuilding ##
		self.log.log("\n", out=True)
		self.log.log("index diagnostics phase 3 (Rebuilding)", out=True)
		if not self.options.chose(rebuildOption):
			self.log.log("  phase skipped.  use -rebuild option to force rebuild", out=True)
			return
		rebuiltIndexName = index.name + ".new"

		self.log.log("  using ancestry info located in previous phase", out=True)

		# Create the output file, read the index and process the batches
		rebuiltIndexf = yield (client.CreateTask(index.tagd, rebuiltIndexName, sattr=nfs3.Sattr3(mode=0700, size=0)), None)
		yield (IndexDiag(index, rebuiltIndexf, Rebuilding), None)

		backupName = index.indexf.name + '.ORIG'
		oldSize = (yield (client.OpenTask(index.tagd, index.indexf.name), None)).a.size
		newSize = (yield (client.OpenTask(index.tagd, rebuiltIndexName), None)).a.size
		self.log.log("  sizes: old index {}; new index {}".format(basics.formatSize(oldSize), basics.formatSize(newSize)), out=True)
		if not self.options.chose(replaceOption):
			self.log.log("  not replacing.  use -rebuild -replace to rebuild the index and replace it", out=True)
			return

		# Make a backup
		self.log.log("  creating backup of existing index - mv {index.indexf} to {backupName}".format(**vars()), out=True)
		yield (index.tagd.rename(index.indexf.name, index.tagd.fh, backupName), None)
		if index.renamef:
			backupName = index.renamef.name + '.ORIG'
			self.log.log("  creating backup of existing 'renames' file - mv {index.indexf} to {backupName}".format(**vars()), out=True)
			yield (index.tagd.rename(index.renamef.name, index.tagd.fh, backupName), None)

		# Replace the index with the rebuilt
		self.log.log("  Renaming {rebuiltIndexName} to {index.indexf}".format(**vars()), out=True)
		yield (index.tagd.rename(rebuiltIndexName, index.tagd.fh, index.indexf.name), None)

indexDiagDesc = command.Desc(
	"indexdiag",
	[
		repo.scanTagOption,
		rebuildOption,
		replaceOption,
		tryMissingOption,
		sched.parallelOption,
		rd.batchOption,
		idx.iBatchOption,
	],
	"Fix the ancestry of an index",
	npaths=None,
	runner=RunIndexDiag,
)

xcp.commands.append((indexDiagDesc, RunIndexDiag))
