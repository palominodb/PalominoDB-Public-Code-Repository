desc "Build tarball"
task :tarball do
  EXCLUDES=["ttt_gui/log/\\*","ttt_gui/public/images/graphs/\\*", "ttt_gui/public/images/slow_queries/\\*"]
  from_rev=ENV['FROM'] ? ENV['FROM'] : nil
  %x{git rev-list --max-count=1 HEAD > .git-version}
  if from_rev
    tree_diff=%x{git diff-tree -r #{from_rev} HEAD .}.split "\n"
    tree_diff.map! { |line| path=line.split(/\s+/)[-1].split("/") ; path.shift ; path.join("/") }
    sh %Q{pushd .. ; tar --exclude #{EXCLUDES.join(" --exclude ")} -cjvf ttt_gui-patch-from-#{from_rev}.tbz2 ttt_gui/.git-version #{tree_diff.join(" ")} ; popd }
  else
    sh %Q{pushd .. ; tar --exclude #{EXCLUDES.join(" --exclude ")} -cjvf ttt_gui.tbz2 ttt_gui ; popd }
  end
  %x{rm .git-version}
end

desc "Apply a patch tarball. Uses .git-version."
task :patch do
  tar_patch=ENV['PATCH'] ? ENV['PATCH'] : nil
  raise Rake::TaskArgumentError, "This task requires PATCH argument" if tar_patch.nil?
  raise Rake::TaskArgumentError, "Patch file not found, or not readable." if !File.exist? tar_patch

  from_rev=tar_patch.split(".")[0].split("-")[-1]
  raise Rake::TaskArgumentError, "No patch version in filename." if from_rev !~ /[a-f0-9]{40}/

  cur_rev=%x{cat .git-version}.chomp
  raise Rake::TaskArgumentError, "Patch version and current version do not patch." if cur_rev != from_rev

  new_rev=%x{tar -O -xjf #{tar_patch} ttt_gui/.git-version}.chomp
  raise Rake::TaskArgumentError, "Already at this patch." if new_rev == cur_rev

  sh "tar --strip-components 1 -xjvf #{tar_patch}"
end
