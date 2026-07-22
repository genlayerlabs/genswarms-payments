Code.require_file(Path.join(__DIR__, "support.exs"))
f = Check.start()
alias Genswarms.Payments.Keccak

hex = fn bin -> Base.encode16(bin, case: :lower) end

# Original Keccak (0x01 padding — what Ethereum uses), NOT SHA3 (0x06).
Check.check(f, "keccak256(\"\") matches Ethereum's empty hash",
  hex.(Keccak.hash_256("")) ==
    "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470")

Check.check(f, "keccak256(\"abc\")",
  hex.(Keccak.hash_256("abc")) ==
    "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45")

# Multi-block absorb: input > 136-byte rate.
long = :binary.copy("a", 200)
Check.check(f, "keccak256 of 200 bytes returns 32 bytes and is deterministic",
  byte_size(Keccak.hash_256(long)) == 32 and
    Keccak.hash_256(long) == Keccak.hash_256(long))

# ERC-20 Transfer event signature — the constant Task 6 hardcodes.
Check.check(f, "keccak256(\"Transfer(address,address,uint256)\") is the ERC-20 topic0",
  hex.(Keccak.hash_256("Transfer(address,address,uint256)")) ==
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

# Pad-edge KAT: 135 bytes mod 136 (@rate) leaves gap == 1 ⇒ the single
# 0x81-byte padding branch (pad/1's `1 -> data <> <<0x81>>` clause), which
# none of the vectors above exercise ("" and "abc" gap != 1; 200 bytes is
# 200 mod 136 = 64, also != 1). Independently computed via ethers.js 6.17.0
# (`ethers.keccak256(Buffer.alloc(135, 0x61))`, NOT this Elixir
# implementation) against 135 bytes of 0x61 ("a").
Check.check(f, "keccak256 of 135 bytes of 'a' — gap==1 single-0x81-pad-byte KAT (ethers.js-verified)",
  hex.(Keccak.hash_256(:binary.copy(<<0x61>>, 135))) ==
    "34367dc248bbd832f4e3e69dfaac2f92638bd0bbd18f2912ba4ef454919cf446")

Check.finish(f)
