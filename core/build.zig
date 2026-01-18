const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Create the main module
    const galleon_mod = b.addModule("galleon", .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true, // Required for cross-compilation and C allocator
    });

    // Static library for Go CGO linking
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "galleon",
        .root_module = galleon_mod,
    });
    lib.linkLibC();
    b.installArtifact(lib);

    // Install header file
    b.installFile("include/galleon.h", "include/galleon.h");

    // Unit tests
    const tests = b.addTest(.{
        .root_module = galleon_mod,
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);

    // Benchmarks
    const bench_mod = b.addModule("bench", .{
        .root_source_file = b.path("src/bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .link_libc = true, // Required for C allocator
    });

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });
    bench.linkLibC();
    b.installArtifact(bench);

    const run_bench = b.addRunArtifact(bench);
    const bench_step = b.step("bench", "Run benchmarks");
    bench_step.dependOn(&run_bench.step);

    // Blitz benchmarks
    const bench_blitz_mod = b.addModule("bench_blitz", .{
        .root_source_file = b.path("src/bench_blitz.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .link_libc = true,
    });

    const bench_blitz = b.addExecutable(.{
        .name = "bench_blitz",
        .root_module = bench_blitz_mod,
    });
    bench_blitz.linkLibC();
    b.installArtifact(bench_blitz);

    const run_bench_blitz = b.addRunArtifact(bench_blitz);
    const bench_blitz_step = b.step("bench-blitz", "Run blitz parallel runtime benchmarks");
    bench_blitz_step.dependOn(&run_bench_blitz.step);

}
