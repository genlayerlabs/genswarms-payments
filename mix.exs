defmodule GenswarmsPayments.MixProject do
  use Mix.Project

  def project do
    [
      app: :genswarms_payments,
      version: "0.1.0",
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      source_url: "https://github.com/genlayerlabs/genswarms-payments",
      description:
        "Payment settlement hub object for genswarms swarms — HD deposit addresses " <>
          "(watch-only xpub), multi-chain USDC watching, idempotent settlement, " <>
          "stamped payment_confirmed delivery to allowlisted targets",
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto]]
  end

  # genswarms is a peer/runtime dependency provided by the host app.
  # curl is a runtime tool dependency.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:decimal, "~> 2.0 or ~> 3.0"},
      {:curvy, "~> 0.3"}
    ]
  end
end
