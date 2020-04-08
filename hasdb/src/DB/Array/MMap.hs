
module DB.Array.MMap where

-- TODO replace the dependency http://hackage.haskell.org/package/mmap
--      *) add `msync` for 'C' and 'D' in 'ACID'
--      *) resolve leakage:
--  https://mail.haskell.org/pipermail/haskell-cafe/2020-April/132061.html
