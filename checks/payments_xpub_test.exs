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

Check.finish(f)
