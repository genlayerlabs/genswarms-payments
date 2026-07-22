Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()

root = Path.expand(Path.join(__DIR__, ".."))
manifest = Jason.decode!(File.read!(Path.join(root, "swarm-object.json")))

lib_files =
  Path.wildcard(Path.join(root, "lib/**/*.ex"))
  |> Enum.map(&Path.relative_to(&1, root))
  |> Enum.sort()

Check.check(f, "manifest module is Genswarms.Payments",
  manifest["module"] == "Genswarms.Payments")
Check.check(f, "manifest files == every lib/**/*.ex (attestation completeness)",
  Enum.sort(manifest["files"]) == lib_files)
Check.check(f, "README exists and names no consumer",
  File.exists?(Path.join(root, "README.md")) and
    not (File.read!(Path.join(root, "README.md")) =~ ~r/wingston|micromarkets/i))

grep = fn pattern ->
  Path.wildcard(Path.join(root, "lib/**/*.ex"))
  |> Enum.any?(&(File.read!(&1) =~ pattern))
end
Check.check(f, "lib/ names no consumer (packages know contracts, not consumers)",
  not grep.(~r/wingston|micromarkets|llm_proxy|llm-proxy/i))

Check.finish(f)
