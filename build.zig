const std = @import("std");
const pkgs = @import("deps.zig").pkgs;

pub fn build(b: *std.build.Builder) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard release options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall.
    const mode = b.standardReleaseOptions();

    const exe = b.addExecutable("lsm-tree", "src/main.zig");
    exe.addPackagePath("serialize", "src/serialize/main.zig");
    exe.addPackagePath("lsmtree", "src/main.zig");
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
    pkgs.addAllTo(main_test);
    main_test.addPackagePath("lsmtree", "src/main.zig");
    main_test.addPackagePath("serialize", "src/serialize/main.zig");
    main_test.addPackage(pkgs.string);
    main_test.setBuildMode(mode);
    const test_step = b.step("test", "run library tests");
    test_step.dependOn(&main_test.step);
}
