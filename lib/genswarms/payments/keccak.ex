defmodule Genswarms.Payments.Keccak do
  @moduledoc """
  Vendored pure-Elixir Keccak-256 (original Keccak 0x01 padding, as Ethereum
  uses — NOT SHA3-256's 0x06). Used only for address derivation and EIP-55
  checksums: once per user, so speed is irrelevant and the no-NIF engine rule
  wins. Verified against known-answer vectors in checks/payments_keccak_test.exs.
  """
  import Bitwise

  @rate 136
  @mask 0xFFFFFFFFFFFFFFFF

  @rc [
    0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
    0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
    0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
    0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
    0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
    0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008
  ]

  # rotation offsets, indexed rot[x][y]
  @rot {
    {0, 36, 3, 41, 18},
    {1, 44, 10, 45, 2},
    {62, 6, 43, 15, 61},
    {28, 55, 25, 21, 56},
    {27, 20, 39, 8, 14}
  }

  @spec hash_256(binary()) :: <<_::256>>
  def hash_256(data) when is_binary(data) do
    state = for x <- 0..4, y <- 0..4, into: %{}, do: {{x, y}, 0}

    data
    |> pad()
    |> chunks()
    |> Enum.reduce(state, &absorb/2)
    |> squeeze()
  end

  defp pad(data) do
    gap = @rate - rem(byte_size(data), @rate)

    case gap do
      1 -> data <> <<0x81>>
      n -> data <> <<0x01>> <> :binary.copy(<<0>>, n - 2) <> <<0x80>>
    end
  end

  defp chunks(<<block::binary-size(@rate), rest::binary>>), do: [block | chunks(rest)]
  defp chunks(<<>>), do: []

  defp absorb(block, state) do
    lanes = for <<lane::little-64 <- block>>, do: lane

    lanes
    |> Enum.with_index()
    |> Enum.reduce(state, fn {lane, i}, st ->
      # lane i sits at x = rem(i,5), y = div(i,5)
      Map.update!(st, {rem(i, 5), div(i, 5)}, &bxor(&1, lane))
    end)
    |> keccak_f()
  end

  defp keccak_f(state), do: Enum.reduce(@rc, state, &round(&2, &1))

  defp round(a, rc) do
    # theta
    c = for x <- 0..4, into: %{}, do:
      {x, Enum.reduce(0..4, 0, fn y, acc -> bxor(acc, a[{x, y}]) end)}

    d = for x <- 0..4, into: %{}, do:
      {x, bxor(c[rem(x + 4, 5)], rotl(c[rem(x + 1, 5)], 1))}

    a = for {{x, y}, v} <- a, into: %{}, do: {{x, y}, bxor(v, d[x])}

    # rho + pi: b[y][(2x+3y) mod 5] = rotl(a[x][y], rot[x][y])
    b =
      for x <- 0..4, y <- 0..4, into: %{} do
        {{y, rem(2 * x + 3 * y, 5)}, rotl(a[{x, y}], elem(elem(@rot, x), y))}
      end

    # chi
    a =
      for x <- 0..4, y <- 0..4, into: %{} do
        {{x, y}, bxor(b[{x, y}], band(bnot(b[{rem(x + 1, 5), y}]) &&& @mask, b[{rem(x + 2, 5), y}]))}
      end

    # iota
    Map.update!(a, {0, 0}, &bxor(&1, rc))
  end

  defp rotl(lane, 0), do: lane

  defp rotl(lane, n) do
    band(bsl(lane, n) ||| bsr(lane, 64 - n), @mask)
  end

  defp squeeze(state) do
    # first 32 bytes of the rate section: lanes (0,0) (1,0) (2,0) (3,0), little-endian
    for i <- 0..3, into: <<>>, do: <<state[{rem(i, 5), div(i, 5)}]::little-64>>
  end
end
