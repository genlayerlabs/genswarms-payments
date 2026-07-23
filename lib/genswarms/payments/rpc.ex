defmodule Genswarms.Payments.Rpc do
  @moduledoc """
  JSON-RPC over curl (the engine has no :inets). The endpoint URL may embed a
  provider API key, so it rides a chmod-600 `--config` tempfile — never argv,
  where `ps` would expose it. The runner seam (`runner: fn args, config_path`)
  keeps checks off the network.

  Scrubbing `chain.rpc_url` out of curl's output is unified HERE, in
  `call/4`, for BOTH the `{:ok, out}` and `{:error, _}` runner return paths —
  `chain` (and thus `rpc_url`) is already in scope here, so there is no need
  for `run_curl/2` to reparse it back out of the config tempfile (a fragile
  `" = "` string split that broke on any URL containing that substring).
  `run_curl/2` returns its raw, unscrubbed output; `call/4` scrubs before
  `parse/1` sees it (success path) or before returning it (error path).
  """
  require Logger

  def call(chain, method, params, opts \\ []) do
    runner = Keyword.get(opts, :runner, &run_curl/2)
    timeout_s = Keyword.get(opts, :timeout_s, 20)

    body = Jason.encode!(%{jsonrpc: "2.0", id: 1, method: method, params: params})

    config_path =
      Path.join(
        System.tmp_dir!(),
        "gsp-rpc-" <> (:crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)) <> ".conf"
      )

    try do
      fd = File.open!(config_path, [:write, :exclusive])
      File.chmod!(config_path, 0o600)
      IO.binwrite(fd, ~s(url = "#{chain.rpc_url}"\n))
      File.close(fd)

      args = [
        "--config", config_path, "--silent", "--show-error", "--fail-with-body",
        "--max-time", Integer.to_string(timeout_s),
        "-H", "content-type: application/json",
        "-X", "POST", "--data", body
      ]

      case runner.(args, config_path) do
        {:ok, out} ->
          parse(scrub(out, chain.rpc_url))

        {:error, why} ->
          {:error, scrub_error(why, chain.rpc_url)}
      end
    after
      File.rm(config_path)
    end
  end

  defp run_curl(args, _config_path) do
    case System.cmd("curl", args, stderr_to_stdout: true) do
      {out, 0} -> {:ok, out}
      {out, code} -> {:error, {:curl, code, out}}
    end
  end

  # Path-aware: replacing only the exact whole rpc_url is not enough —
  # provider error bodies (curl --fail-with-body) and not-JSON responses
  # routinely echo just the URL PATH (`/v2/<APIKEY>`), and for keyed
  # endpoints (Alchemy/Infura style) the API key lives in that path (or in
  # userinfo/query). Redact every one of those fragments, longest first so
  # the whole-URL replacement doesn't leave a partial behind.
  defp scrub(str, url) when is_binary(str) do
    Enum.reduce(secret_fragments(url), str, fn frag, acc ->
      String.replace(acc, frag, "[redacted]")
    end)
  end

  defp scrub(other, _url), do: other

  defp secret_fragments(url) do
    uri = URI.parse(url)

    [url, uri.path, uri.userinfo, uri.query]
    |> Enum.filter(&(is_binary(&1) and byte_size(&1) > 1))
    |> Enum.uniq()
    |> Enum.sort_by(&byte_size/1, :desc)
  end

  # Only the {:curl, code, msg} shape run_curl/2 itself emits carries a
  # string that could contain the URL — an injected custom runner may return
  # any other opaque {:error, why} term (e.g. a plain atom), which is left
  # untouched since there's nothing scrubbable in it.
  defp scrub_error({:curl, code, msg}, url) when is_binary(msg) do
    {:curl, code, scrub(msg, url) |> String.slice(0, 200)}
  end

  defp scrub_error(other, _url), do: other

  defp parse(out) do
    case Jason.decode(out) do
      {:ok, %{"result" => result}} -> {:ok, result}
      {:ok, %{"error" => err}} -> {:error, {:rpc, err["code"], err["message"]}}
      {:ok, other} -> {:error, {:bad_response, other}}
      {:error, _} -> {:error, {:not_json, String.slice(out, 0, 200)}}
    end
  end
end
