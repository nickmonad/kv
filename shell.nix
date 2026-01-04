{ pkgs ? import <nixpkgs> {} }:

let
    zig = pkgs.stdenv.mkDerivation {
        name = "zig";
        src = pkgs.fetchurl {
            url = "https://ziglang.org/download/0.15.2/zig-x86_64-linux-0.15.2.tar.xz";
            hash = "sha256-AqonDxg9onbltZILHaxEpj8aSeVQUOveOuzJ64L5Mjk=";
        };

        nativeBuildInputs = [
            pkgs.autoPatchelfHook
        ];

        buildInputs = [
            pkgs.stdenv.cc.cc.lib
            pkgs.zlib
        ];

        installPhase = ''
            mkdir -p $out/bin
            cp -r . $out/zig-install
            ln -s $out/zig-install/zig $out/bin/zig
        '';
    };
in
pkgs.mkShell {
    buildInputs = [
        zig
        pkgs.redis
    ];

    shellHook = ''
        echo "zig $(zig version)"
        echo "$(redis-cli --version)"
    '';
}
