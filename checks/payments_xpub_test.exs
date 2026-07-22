Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments.HD

xpub = "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

{:ok, parsed} = HD.parse_xpub(xpub)
Check.check(f, "chain_code is 32 bytes", byte_size(parsed.chain_code) == 32)
Check.check(f, "pubkey is 33 bytes compressed (0x02/0x03 prefix)",
  byte_size(parsed.pubkey) == 33 and :binary.first(parsed.pubkey) in [2, 3])
Check.check(f, "depth is 3 (m/44'/60'/0')", parsed.depth == 3)

Check.check(f, "bad checksum rejected",
  HD.parse_xpub(String.slice(xpub, 0..-2//1) <> "z") == {:error, :bad_checksum})
Check.check(f, "garbage rejected", match?({:error, _}, HD.parse_xpub("notanxpub")))
Check.check(f, "xprv rejected (never accept private key material)",
  HD.parse_xpub(
    "xprv9s21ZrQH143K3QTDL4LXw2F7HEK3wJUD2nW2nRk4stbPy6cq3jPPqjiChkVvvNKmPGJxWUtg6LnF5kejMRNNU3TGtRBeJgk33yuGBxrMPHi"
  ) == {:error, :not_an_xpub})

# --- Regression: checksum-VALID, right-length, right-version payload but a
# pubkey prefix byte that is neither 0x02 nor 0x03 must still be rejected as
# :not_an_xpub (NOT :bad_length — the payload genuinely is 78 bytes; it is
# the pubkey shape that disqualifies it). Local base58 encode/decode
# helpers below let us craft-and-re-checksum such a payload without
# reaching into HD's private functions.
b58_alphabet = ~c(123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz)

decode_b58 = fn str ->
  chars = String.to_charlist(str)
  index = fn c -> Enum.find_index(b58_alphabet, &(&1 == c)) end
  int = Enum.reduce(chars, 0, fn c, acc -> acc * 58 + index.(c) end)
  zeros = Enum.take_while(chars, &(&1 == ?1)) |> length()
  :binary.copy(<<0>>, zeros) <> :binary.encode_unsigned(int)
end

encode_b58 = fn bin ->
  leading_zeros =
    bin |> :binary.bin_to_list() |> Enum.take_while(&(&1 == 0)) |> length()

  int = :binary.decode_unsigned(bin)

  digits =
    Stream.unfold(int, fn
      0 -> nil
      n -> {rem(n, 58), div(n, 58)}
    end)
    |> Enum.to_list()
    |> Enum.reverse()
    |> Enum.map(&Enum.at(b58_alphabet, &1))

  (List.duplicate(?1, leading_zeros) ++ digits) |> List.to_string()
end

real_body = decode_b58.(xpub)
<<real_payload::binary-78, _real_checksum::binary-4>> = real_body

# offset 45: 4 version + 1 depth + 4 fingerprint + 4 child + 32 chain_code
<<prefix::binary-45, _bad_prefix_byte::binary-1, rest::binary-32>> = real_payload
crafted_payload = prefix <> <<4>> <> rest

<<crafted_checksum::binary-4, _::binary>> =
  :crypto.hash(:sha256, :crypto.hash(:sha256, crafted_payload))

crafted_xpub = encode_b58.(crafted_payload <> crafted_checksum)

Check.check(
  f,
  "checksum-valid payload with bad pubkey prefix byte -> :not_an_xpub (not :bad_length)",
  HD.parse_xpub(crafted_xpub) == {:error, :not_an_xpub}
)

Check.finish(f)
