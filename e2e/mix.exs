defmodule GenswarmsPaymentsE2E.MixProject do
  use Mix.Project

  # Cross-package end-to-end harness: boots the REAL genswarms-payments
  # settlement hub and the REAL genswarms-llm-proxy in one BEAM and drives
  # the full USDC -> credit -> spend user story across the live seam.
  # The proxy checkout is located via LLM_PROXY_PATH (absolute path
  # recommended), defaulting to the sibling checkout ../genswarms-llm-proxy
  # relative to the payments repo root (= ../../genswarms-llm-proxy from
  # this e2e/ project).
  def project do
    [
      app: :genswarms_payments_e2e,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: false,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto]]
  end

  defp deps do
    [
      {:genswarms_payments, path: ".."},
      {:genswarms_llm_proxy,
       path: System.get_env("LLM_PROXY_PATH") || "../../genswarms-llm-proxy"}
    ]
  end
end
