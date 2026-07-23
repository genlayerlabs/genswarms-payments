Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments.Rpc

{:ok, captured} = Agent.start_link(fn -> nil end)

runner = fn args, config_path ->
  Agent.update(captured, fn _ -> {args, File.read!(config_path)} end)
  {:ok, ~s({"jsonrpc":"2.0","id":1,"result":"0xc8"})}
end

chain = %{name: "base", rpc_url: "https://mainnet.base.org/v2/SECRETKEY"}
{:ok, result} = Rpc.call(chain, "eth_blockNumber", [], runner: runner)

Check.check(f, "parses result", result == "0xc8")

{args, config_contents} = Agent.get(captured, & &1)
Check.check(f, "URL never on argv (rides the --config tempfile)",
  not Enum.any?(args, &String.contains?(&1, "SECRETKEY")) and
    String.contains?(config_contents, "SECRETKEY"))
Check.check(f, "POSTs JSON-RPC 2.0 body",
  Enum.any?(args, &String.contains?(&1, "eth_blockNumber")))

err_runner = fn _, _ -> {:ok, ~s({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}})} end
Check.check(f, "RPC error object becomes {:error, _}",
  match?({:error, _}, Rpc.call(chain, "eth_blockNumber", [], runner: err_runner)))

Check.check(f, "curl failure becomes {:error, _}",
  match?({:error, _}, Rpc.call(chain, "eth_blockNumber", [], runner: fn _, _ -> {:error, :curl_28} end)))

not_json_runner = fn _, _ -> {:ok, "This request to https://mainnet.base.org/v2/SECRETKEY failed"} end
{:error, {:not_json, slice}} = Rpc.call(chain, "eth_blockNumber", [], runner: not_json_runner)
Check.check(f, "error output scrubs URL (no SECRETKEY in :not_json slice)",
  not String.contains?(slice, "SECRETKEY"))

# ── 3a: scrubbing is unified in call/4 (BOTH the {:ok, out} and {:error, _}
# runner paths), not reparsed out of the config file inside run_curl —
# exercise the shape run_curl itself returns for a nonzero curl exit.
curl_exit_runner = fn _args, _config_path ->
  {:error, {:curl, 22, "curl: (22) https://mainnet.base.org/v2/SECRETKEY returned 500"}}
end

{:error, {:curl, 22, curl_exit_msg}} = Rpc.call(chain, "eth_blockNumber", [], runner: curl_exit_runner)

Check.check(f, "curl-exit-nonzero error tuple has the rpc_url scrubbed",
  is_binary(curl_exit_msg) and not String.contains?(curl_exit_msg, "SECRETKEY"))

# ── 3b: path-aware scrub. Provider error bodies routinely echo only the URL
# PATH (`/v2/<APIKEY>`) — where Alchemy/Infura-style keys live — not the full
# origin. A whole-URL-only replace leaks the key into {:error, _} tuples that
# Usdc.poll logs verbatim. Cover BOTH the {:curl, ...} error path and the
# not-JSON success path with a body containing only the path.
path_only_curl_runner = fn _args, _config_path ->
  {:error, {:curl, 22, "curl: (22) The requested URL /v2/SECRETKEY returned error: 401"}}
end

{:error, {:curl, 22, path_only_msg}} =
  Rpc.call(chain, "eth_blockNumber", [], runner: path_only_curl_runner)

Check.check(f, "3b: error body echoing only the URL PATH is scrubbed (no key fragment)",
  is_binary(path_only_msg) and not String.contains?(path_only_msg, "SECRETKEY") and
    not String.contains?(path_only_msg, "/v2/SECRETKEY"))

path_only_not_json_runner = fn _, _ ->
  {:ok, "404 page not found: /v2/SECRETKEY does not exist"}
end

{:error, {:not_json, path_only_slice}} =
  Rpc.call(chain, "eth_blockNumber", [], runner: path_only_not_json_runner)

Check.check(f, "3b: not-json body echoing only the URL PATH is scrubbed (no key fragment)",
  not String.contains?(path_only_slice, "SECRETKEY") and
    not String.contains?(path_only_slice, "/v2/SECRETKEY"))

# userinfo- and query-keyed URLs leak the same way — pin those fragments too
keyed_chain = %{name: "base", rpc_url: "https://user:QUERYPASS@node.example.org/rpc?apikey=QUERYKEY"}

keyed_runner = fn _, _ ->
  {:ok, "unauthorized for user:QUERYPASS with apikey=QUERYKEY"}
end

{:error, {:not_json, keyed_slice}} =
  Rpc.call(keyed_chain, "eth_blockNumber", [], runner: keyed_runner)

Check.check(f, "3b: userinfo and query fragments of the rpc_url are scrubbed too",
  not String.contains?(keyed_slice, "QUERYPASS") and
    not String.contains?(keyed_slice, "QUERYKEY"))

Check.finish(f)
