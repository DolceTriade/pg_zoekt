{
  rustPlatform,
  buildPgrxExtension,
  cargo-pgrx_0_16_1,
  postgresql,
  fetchFromGitHub,
  lib,
}: let
  buildPgrxExtension' = buildPgrxExtension.override {
    inherit rustPlatform;
  };
in
  buildPgrxExtension' (finalAttrs: {
    pname = "pg_zoekt";
    version = "0.0.1";
    enableParallelBuilding = true;
    src = ../.;

    doCheck = false;
    cargoLock = {
      lockFile = ../Cargo.lock;
    };

    cargoHash = "";

    cargoPgrxFlags = [
      "-p"
      "pg_zoekt"
    ];

    inherit postgresql;
    cargo-pgrx = cargo-pgrx_0_16_1;

    meta = {
      homepage = "https://github.com/DolceTriade/pg_zoekt";
      teams = [];
      description = "Zoekt";
      license = lib.licenses.postgresql;
      platforms = postgresql.meta.platforms;
    };
  })
