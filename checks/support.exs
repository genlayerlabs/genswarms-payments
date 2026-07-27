defmodule Check do
  def start do
    {:ok, failures} = Agent.start_link(fn -> [] end)
    failures
  end

  def check(failures, label, ok) do
    if ok do
      IO.puts("  ok   #{label}")
    else
      IO.puts("  FAIL #{label}")
      Agent.update(failures, &[label | &1])
    end
  end

  def finish(failures) do
    failed = Agent.get(failures, & &1)
    if failed != [], do: System.halt(1)
  end

  # D4: before poll/1 scans a chain it makes the endpoint prove its identity
  # (eth_chainId + the token contract's decimals()). Every rpc_fn stub driven
  # through poll must therefore answer those two calls, or the chain is held
  # and nothing scans. This helper answers them truthfully FROM the chain
  # config; a check that wants a MISMATCH overrides the clause locally.
  def self_check_rpc(chain, "eth_chainId"),
    do: {:ok, "0x" <> Integer.to_string(Map.fetch!(chain, :chain_id), 16)}

  def self_check_rpc(chain, "eth_call") do
    decimals = Map.get(chain, :decimals, 6)
    {:ok, "0x" <> String.pad_leading(Integer.to_string(decimals, 16), 64, "0")}
  end

  def self_check_methods, do: ["eth_chainId", "eth_call"]
end
