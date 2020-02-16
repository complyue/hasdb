# this defines the Nix env to run this Haskell project
with (import ./. { });
haskellPackages.hasdb.envFunc { withHoogle = true; }
