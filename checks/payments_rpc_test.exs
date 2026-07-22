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

Check.finish(f)
