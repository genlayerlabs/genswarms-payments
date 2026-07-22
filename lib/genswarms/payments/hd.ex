defmodule Genswarms.Payments.HD do
  @moduledoc """
  Watch-only BIP32: xpub parsing and non-hardened public child derivation to
  Ethereum addresses. No private-key material is ever accepted or held —
  servers can watch, never spend. Pure Elixir (curvy + vendored keccak).
  """

  alias Genswarms.Payments.Keccak

  @b58_alphabet ~c(123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz)
  # mainnet public version bytes 0x0488B21E ("xpub")
  @xpub_version <<0x04, 0x88, 0xB2, 0x1E>>

  @spec parse_xpub(String.t()) ::
          {:ok, %{chain_code: binary(), pubkey: binary(), depth: byte()}} | {:error, atom()}
  def parse_xpub(xpub) when is_binary(xpub) do
    with {:ok, payload} <- base58check_decode(xpub) do
      case payload do
        <<@xpub_version, depth::8, _fingerprint::binary-4, _child::binary-4,
          chain_code::binary-32, pubkey::binary-33>> ->
          if :binary.first(pubkey) in [2, 3] do
            {:ok, %{chain_code: chain_code, pubkey: pubkey, depth: depth}}
          else
            {:error, :not_an_xpub}
          end

        <<_version::binary-4, _rest::binary-74>> ->
          {:error, :not_an_xpub}

        _ ->
          {:error, :bad_length}
      end
    end
  end

  defp base58check_decode(str) do
    chars = String.to_charlist(str)

    if Enum.all?(chars, &(&1 in @b58_alphabet)) do
      int = Enum.reduce(chars, 0, fn c, acc -> acc * 58 + index58(c) end)
      zeros = Enum.take_while(chars, &(&1 == ?1)) |> length()
      body = :binary.copy(<<0>>, zeros) <> :binary.encode_unsigned(int)
      body_size = byte_size(body)

      if body_size > 4 do
        payload_size = body_size - 4
        <<payload::binary-size(payload_size), checksum::binary-4>> = body

        <<expect::binary-4, _::binary>> =
          :crypto.hash(:sha256, :crypto.hash(:sha256, payload))

        if checksum == expect, do: {:ok, payload}, else: {:error, :bad_checksum}
      else
        {:error, :bad_length}
      end
    else
      {:error, :bad_base58}
    end
  end

  for {c, i} <- Enum.with_index(@b58_alphabet) do
    defp index58(unquote(c)), do: unquote(i)
  end

  @doc """
  Ethereum address for external-chain child `m/<xpub>/0/<index>` (BIP44 change
  level 0). Non-hardened public derivation only — hardened is impossible
  without the private key, reject loudly.
  """
  @spec address(map(), non_neg_integer()) :: {:ok, String.t()} | {:error, atom()}
  def address(%{chain_code: _, pubkey: _} = parsed, index)
      when is_integer(index) and index >= 0 do
    if index >= 0x80000000 do
      {:error, :hardened_index}
    else
      with {:ok, change} <- ckd_pub(parsed, 0),
           {:ok, child} <- ckd_pub(change, index) do
        {:ok, eth_address(child.pubkey)}
      end
    end
  end

  # CKDpub: I = HMAC-SHA512(chain_code, serP(K_par) || ser32(i));
  # child K = point(IL) + K_par  (BIP32)
  defp ckd_pub(%{chain_code: cc, pubkey: pubkey}, i) do
    <<il::binary-32, ir::binary-32>> =
      :crypto.mac(:hmac, :sha512, cc, pubkey <> <<i::32>>)

    parent = Curvy.Key.from_pubkey(pubkey)
    tweak = Curvy.Key.from_privkey(il)
    child_point = Curvy.Point.add(tweak.point, parent.point)

    child_pub =
      %Curvy.Key{point: child_point} |> Curvy.Key.to_pubkey(compressed: true)

    {:ok, %{chain_code: ir, pubkey: child_pub}}
  end

  defp eth_address(compressed_pubkey) do
    # uncompress via curvy, drop the 0x04 prefix, keccak, last 20 bytes
    <<4, xy::binary-64>> =
      compressed_pubkey
      |> Curvy.Key.from_pubkey()
      |> Curvy.Key.to_pubkey(compressed: false)

    <<_::binary-12, raw::binary-20>> = Keccak.hash_256(xy)
    eip55(raw)
  end

  defp eip55(<<raw::binary-20>>) do
    hex = Base.encode16(raw, case: :lower)
    hash = Keccak.hash_256(hex) |> Base.encode16(case: :lower)

    cased =
      hex
      |> String.graphemes()
      |> Enum.zip(String.graphemes(hash))
      |> Enum.map_join(fn {c, h} ->
        if h >= "8", do: String.upcase(c), else: c
      end)

    "0x" <> cased
  end

  # keep Keccak referenced from Task 3 on (used by Task 4)
  @doc false
  def keccak_mod, do: Keccak
end
