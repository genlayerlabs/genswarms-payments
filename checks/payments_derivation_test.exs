Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments.HD

xpub =
  "xpub6DCoCpSuQZB2jawqnGMEPS63ePKWkwWPH4TU45Q7LPXWuNd8TMtVxRrgjtEshuqpK3mdhaWHPFsBngh5GFZaM6si3yZdUsT8ddYM3PwnATt"

addr0 = "0x9858EfFD232B4033E47d90003D41EC34EcaEda94"
addr1 = "0x6Fac4D18c912343BF86fa7049364Dd4E424Ab9C0"
addr7 = "0x593814d3309e2dF31D112824F0bb5aa7Cb0D7d47"

{:ok, parsed} = HD.parse_xpub(xpub)

{:ok, a0} = HD.address(parsed, 0)
{:ok, a1} = HD.address(parsed, 1)
{:ok, a7} = HD.address(parsed, 7)

Check.check(f, "index 0 matches ethers.js", a0 == addr0)
Check.check(f, "index 1 matches ethers.js", a1 == addr1)
Check.check(f, "index 7 matches ethers.js", a7 == addr7)
Check.check(f, "EIP-55 mixed case present (not all-lower)",
  a0 != String.downcase(a0))
Check.check(f, "deterministic", HD.address(parsed, 0) == {:ok, a0})
Check.check(f, "hardened index rejected",
  HD.address(parsed, 0x80000000) == {:error, :hardened_index})

Check.finish(f)
