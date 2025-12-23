{
  description = "pg_zoekt -- Positional trigram index for Postgresql";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs @ {flake-parts, ...}:
    flake-parts.lib.mkFlake {inherit inputs;} {
      imports = [
        inputs.flake-parts.flakeModules.easyOverlay
        # To import an internal flake module: ./other.nix
        # To import an external flake module:
        #   1. Add foo to inputs
        #   2. Add foo as a parameter to the outputs function
        #   3. Add here: foo.flakeModule
      ];
      systems = ["x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin"];
      perSystem = {
        config,
        self',
        inputs',
        pkgs,
        system,
        ...
      }: {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [inputs.fenix.overlays.default];
        };
        packages.pg_zoekt = let
          rust = with pkgs.fenix;
          with latest;
            combine [
              cargo
              rustc
            ];
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rust;
            rustc = rust;
          };
        in
          pkgs.callPackage ./nix/pg_zoekt.nix {
            inherit rustPlatform;
            postgresql = pkgs.postgresql_18;
          };
        packages.default = self'.packages.pg_zoekt;
        overlayAttrs = {
          pg_zoekt = self'.packages.pg_zoekt;
        };
      };
      flake = {
        # The usual flake attributes can be defined here, including system-
        # agnostic ones like nixosModule and system-enumerating ones, although
        # those are more easily expressed in perSystem.
      };
    };
}
