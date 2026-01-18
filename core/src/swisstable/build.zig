const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Library module - can be imported by other Zig projects
    // Uses mod.zig as entry point which exports both SwissTable and SwissSet
    const swisstable_mod = b.addModule("swisstable", .{
        .root_source_file = b.path("mod.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Static library artifact
    const lib = b.addLibrary(.{
        .linkage = .static,
        .name = "swisstable",
        .root_module = swisstable_mod,
    });
    b.installArtifact(lib);

    // Unit tests
    const tests = b.addTest(.{
        .root_module = swisstable_mod,
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run Swiss Table unit tests");
    test_step.dependOn(&run_tests.step);

    // Benchmark executable
    const bench_mod = b.addModule("bench", .{
        .root_source_file = b.path("bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
        .link_libc = true,
    });

    const bench = b.addExecutable(.{
        .name = "bench_swisstable",
        .root_module = bench_mod,
    });
    bench.linkLibC();
    b.installArtifact(bench);

    const run_bench = b.addRunArtifact(bench);
    const bench_step = b.step("bench", "Run Swiss Table benchmarks");
    bench_step.dependOn(&run_bench.step);
}
