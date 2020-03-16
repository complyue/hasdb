
module DB.Storage.InMem.TreeIdx
  ( foldRange
  )
where

import           Prelude

import           Data.Maybe

import           Data.Map.Internal


-- Kudos: Olaf Klinke olf at aatal-apotheke.de
--   https://mail.haskell.org/pipermail/haskell-cafe/2020-March/132004.html


-- name clash with Control.Monad
when :: (Monoid b) => Bool -> b -> b
when t b = if t then b else mempty

contains :: Ord k => (Maybe k, Maybe k) -> k -> Bool
contains (lbound, ubound) k =
  (isNothing lbound || fromJust lbound <= k)
    && (isNothing ubound || k <= fromJust ubound)

foldRange
  :: (Monoid b, Ord k) => ((k, a) -> b) -> (Maybe k, Maybe k) -> Map k a -> b
foldRange f range@(lbound, ubound) m = case m of
  Tip                    -> mempty
  (Bin _ k a left right) -> foldLeft <> this <> foldRight   where
    foldLeft =
      when (isNothing lbound || fromJust lbound < k) (foldRange f range left)
    this = when (range `contains` k) (f (k, a))
    foldRight =
      when (isNothing ubound || k < fromJust ubound) (foldRange f range right)

