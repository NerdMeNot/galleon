const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Main library module
    const blitz_mod = b.addModule("blitz", .{
        .root_source_file = b.path("mod.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Static library for linking
    const lib = b.*.addStaticLibrary(.{
        .name = "blitz",
        .root_source_file = b.path("mod.zig"),
        .target = target,
        .optimize = optimize,
    });

    b.*.installArtifact(lib);

    // ============================================================================
    // Tests
    // ============================================================================

    const test_step = b.step("test", "Run unit tests");

    // Test all modules
    const test_files = [_][]const u8{
        "mod.zig",
        "api.zig",
        "pool.zig",
        "job.zig",
        "worker.zig",
        "future.zig",
        "latch.zig",
        "sync.zig",
        "threshold.zig",
    };

    for (test_files) |file| {
        const unit_tests = b.addTest(.{
            .root_source_file = b.path(file),
            .target = target,
            .optimize = optimize,
        });
        const run_tests = b.addRunArtifact(unit_tests);
        test_step.dependOn(&run_tests.step);
    }

    // ============================================================================
    // Benchmarks
    // ============================================================================

    const bench_step = b.step("bench", "Run benchmarks");

    const bench = b.addExecutable(.{
        .name = "blitz_bench",
        .root_source_file = b.path("bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench.root_module.addImport("blitz", blitz_mod);

    const run_bench = b.addRunArtifact(bench);
    if (b.args) |args| {
        run_bench.addArgs(args);
    }
    bench_step.dependOn(&run_bench.step);

    b.installArtifact(bench);
}
