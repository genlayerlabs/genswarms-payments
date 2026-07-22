defmodule Genswarms.Payments.Rpc do
  @moduledoc """
  JSON-RPC over curl (the engine has no :inets). The endpoint URL may embed a
  provider API key, so it rides a chmod-600 `--config` tempfile — never argv,
  where `ps` would expose it. The runner seam (`runner: fn args, config_path`)
  keeps checks off the network.
  """
  require Logger

  def call(chain, method, params, opts \\ []) do
    runner = Keyword.get(opts, :runner, &run_curl/2)
    timeout_s = Keyword.get(opts, :timeout_s, 20)

    body = Jason.encode!(%{jsonrpc: "2.0", id: 1, method: method, params: params})

    config_path =
      Path.join(System.tmp_dir!(), "gsp-rpc-#{:erlang.unique_integer([:positive])}.conf")

    File.write!(config_path, ~s(url = "#{chain.rpc_url}"\n))
    File.chmod!(config_path, 0o600)

    args = [
      "--config", config_path, "--silent", "--show-error", "--fail-with-body",
      "--max-time", Integer.to_string(timeout_s),
      "-H", "content-type: application/json",
      "-X", "POST", "--data", body
    ]

    try do
      case runner.(args, config_path) do
        {:ok, out} -> parse(out)
        {:error, why} -> {:error, why}
      end
    after
      File.rm(config_path)
    end
  end

  defp run_curl(args, _config_path) do
    case System.cmd("curl", args, stderr_to_stdout: true) do
      {out, 0} -> {:ok, out}
      {out, code} -> {:error, {:curl, code, String.slice(out, 0, 200)}}
    end
  end

  defp parse(out) do
    case Jason.decode(out) do
      {:ok, %{"result" => result}} -> {:ok, result}
      {:ok, %{"error" => err}} -> {:error, {:rpc, err["code"], err["message"]}}
      {:ok, other} -> {:error, {:bad_response, other}}
      {:error, _} -> {:error, {:not_json, String.slice(out, 0, 200)}}
    end
  end
end
