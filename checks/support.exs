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
end
