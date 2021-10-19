const std = @import("std");
const Pkg = std.build.Pkg;
const FileSource = std.build.FileSource;

pub const pkgs = struct {
    pub const json = Pkg{
        .name = "json",
        .path = FileSource{
            .path = ".gyro/json-getty-zig-github-3a2996a3804697b3645cb17f8f1284cae168e145/pkg/src/lib.zig",
        },
        .dependencies = &[_]Pkg{
            Pkg{
                .name = "getty",
                .path = FileSource{
                    .path = ".gyro/getty-getty-zig-github-8f5afdfa3dacbff69534cab620696836774386a0/pkg/src/lib.zig",
                },
            },
        },
    };

    pub fn addAllTo(artifact: *std.build.LibExeObjStep) void {
        artifact.addPackage(pkgs.json);
    }
};
