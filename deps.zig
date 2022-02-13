const std = @import("std");
const Pkg = std.build.Pkg;
const FileSource = std.build.FileSource;

pub const pkgs = struct {
    pub const string = Pkg{
        .name = "string",
        .path = FileSource{
            .path = ".gyro/zig-string-JakubSzark-github.com-6678e7a0/pkg/zig-string.zig",
        },
    };

    pub fn addAllTo(artifact: *std.build.LibExeObjStep) void {
        artifact.addPackage(pkgs.string);
    }
};

pub const exports = struct {
    pub const @"lsm-tree" = Pkg{
        .name = "lsm-tree",
        .path = "src/main.zig",
        .dependencies = &[_]Pkg{
            pkgs.string,
        },
    };
};
