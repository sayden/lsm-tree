const std = @import("std");
const pkgs = @import("deps.zig").pkgs;
const Pkg = std.build.Pkg;
const nanoid = @import("./src/pkg/zig-nanoid/build.zig");

pub fn build(b: *std.build.Builder) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    const exe = b.addExecutable("lsmtree", "src/main.zig");
    const root = Pkg{ .name = "lsmtree", .path = .{ .path = "src/main.zig" } };
    const strings = Pkg{ .name = "strings", .path = .{ .path = "./src/pkg/pkg/strings/strings.zig" } };
    const serialize = Pkg{ .name = "serialize", .path = .{ .path = "./src/pkg/serialize/main.zig" }, .dependencies = &[_]Pkg{root} };
    // const nanoid = Pkg{ .name = "nanoid", .path = .{ .path = "./src/pkg/nanoid/build.zig" }, .dependencies = &[_]Pkg{root} };

    exe.addPackage(strings);
    exe.addPackage(serialize);
    exe.addPackage(nanoid.getPackage("nanoid"));

    exe.setTarget(target);
    exe.setBuildMode(mode);
    pkgs.addAllTo(exe);
    exe.install();

    const run_cmd = exe.run();

    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the app");
    run_step.dependOn(&run_cmd.step);

    var main_test = b.addTest("src/test.zig");
    main_test.addPackage(serialize);
    main_test.addPackage(root);
    main_test.setBuildMode(mode);
    pkgs.addAllTo(main_test);

    const test_step = b.step("test", "run library tests");
    test_step.dependOn(&main_test.step);
}
