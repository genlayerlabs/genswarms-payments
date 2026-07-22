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

    try do
      File.touch!(config_path)
      File.chmod!(config_path, 0o600)
      File.write!(config_path, ~s(url = "#{chain.rpc_url}"\n))

      args = [
        "--config", config_path, "--silent", "--show-error", "--fail-with-body",
        "--max-time", Integer.to_string(timeout_s),
        "-H", "content-type: application/json",
        "-X", "POST", "--data", body
      ]

      case runner.(args, config_path) do
        {:ok, out} ->
          scrubbed = String.replace(out, chain.rpc_url, "[rpc-url]")
          parse(scrubbed)
        {:error, why} -> {:error, why}
      end
    after
      File.rm(config_path)
    end
  end

  defp run_curl(args, config_path) do
    case System.cmd("curl", args, stderr_to_stdout: true) do
      {out, 0} ->
        {:ok, out}

      {out, code} ->
        # Read the URL from the config to scrub it from error output
        rpc_url = File.read!(config_path) |> String.split("\n") |> Enum.find_value(fn line ->
          case String.split(line, " = ") do
            [_key, value] -> String.trim(value, "\"")
            _ -> nil
          end
        end)
        scrubbed_out = if rpc_url, do: String.replace(out, rpc_url, "[rpc-url]"), else: out
        {:error, {:curl, code, String.slice(scrubbed_out, 0, 200)}}
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
